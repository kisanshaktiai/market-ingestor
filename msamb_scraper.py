"""
Universal Multi-State APMC Market Price Scraper
Author: Amarsinh Patil (KisanShaktiAI)

Architecture: Strategy Pattern for handling different state website structures
- Each state has its own scraper class
- Configuration-driven from agri_market_sources table
- Pluggable adapters for different website types
"""

import os
import sys
import time
import logging
import datetime
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Type
from abc import ABC, abstractmethod
import json

from supabase import create_client, Client

# -----------------------------------
# Configuration & Logging
# -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('apmc_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


# -----------------------------------
# Supabase Manager
# -----------------------------------
class SupabaseManager:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)
        logger.info("✅ Supabase client initialized")
    
    def get_active_sources(self, state_code: Optional[str] = None) -> List[Dict]:
        """Get all active scraper configurations"""
        try:
            query = self.supabase.table('agri_market_sources').select('*').eq('active', True)
            
            if state_code:
                query = query.eq('state_code', state_code)
            
            response = query.execute()
            logger.info(f"✅ Loaded {len(response.data)} active source(s)")
            return response.data
        except Exception as e:
            logger.error(f"❌ Error fetching sources: {e}")
            return []
    
    def get_commodity_mapping(self) -> Dict[str, Dict]:
        """Load commodity master for normalization"""
        try:
            response = self.supabase.table('commodity_master').select('*').execute()
            mapping = {}
            for row in response.data:
                mapping[row['name'].lower()] = row
                if row.get('aliases'):
                    for alias in row['aliases']:
                        mapping[alias.lower()] = row
            logger.info(f"✅ Loaded {len(response.data)} commodities")
            return mapping
        except Exception as e:
            logger.error(f"❌ Error loading commodities: {e}")
            return {}
    
    def upsert_market_prices(self, records: List[Dict], batch_size: int = 100) -> int:
        """Bulk upsert market prices"""
        if not records:
            return 0
        
        total = 0
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.supabase.table('market_prices').upsert(batch).execute()
                total += len(batch)
                logger.info(f"✔ Upserted batch: {len(batch)} records")
                time.sleep(0.5)
            return total
        except Exception as e:
            logger.error(f"❌ Error upserting: {e}")
            raise
    
    def update_source_status(self, source_id: str, status: str, error: Optional[str] = None):
        """Update source check time and status"""
        try:
            update_data = {
                'last_checked_at': datetime.datetime.now().isoformat(),
            }
            if error:
                update_data['notes'] = f"Last error: {error}"
            
            self.supabase.table('agri_market_sources').update(update_data).eq('id', source_id).execute()
        except Exception as e:
            logger.error(f"⚠️ Error updating source status: {e}")


# -----------------------------------
# Base Scraper (Abstract Class)
# -----------------------------------
class BaseAPMCScraper(ABC):
    """Abstract base class for state-specific scrapers"""
    
    def __init__(self, db_manager: SupabaseManager, config: Dict):
        self.db = db_manager
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.commodity_mapping = db_manager.get_commodity_mapping()
        self.state_code = config.get('state_code')
        self.organization = config.get('organization')
        
    @abstractmethod
    def fetch_commodities(self) -> Dict[str, str]:
        """Fetch list of commodities from source - MUST BE IMPLEMENTED BY EACH STATE"""
        pass
    
    @abstractmethod
    def fetch_commodity_data(self, commodity_code: str, commodity_name: str) -> Optional[str]:
        """Fetch raw data for a commodity - MUST BE IMPLEMENTED BY EACH STATE"""
        pass
    
    @abstractmethod
    def parse_data(self, raw_data: str, commodity_code: str, commodity_name: str) -> List[Dict]:
        """Parse raw data into structured records - MUST BE IMPLEMENTED BY EACH STATE"""
        pass
    
    def normalize_commodity(self, name: str) -> Dict:
        """Normalize commodity name"""
        key = name.lower().strip()
        if key in self.commodity_mapping:
            mapped = self.commodity_mapping[key]
            return {
                'global_code': mapped['global_code'],
                'name': mapped['name'],
                'category': mapped['category']
            }
        return {'global_code': None, 'name': name, 'category': None}
    
    def clean_numeric(self, value: str) -> Optional[float]:
        """Clean numeric value"""
        if not value or value == '-':
            return None
        try:
            import re
            cleaned = re.sub(r'[^0-9.]', '', value)
            return float(cleaned) if cleaned else None
        except:
            return None
    
    def run(self) -> bool:
        """Main execution flow - same for all states"""
        logger.info(f"🚀 Starting scraper for {self.organization} ({self.state_code})")
        
        try:
            # Step 1: Fetch commodities
            commodities = self.fetch_commodities()
            if not commodities:
                logger.error(f"❌ No commodities loaded for {self.organization}")
                self.db.update_source_status(self.config['id'], 'failed', 'No commodities found')
                return False
            
            logger.info(f"✅ Loaded {len(commodities)} commodities")
            
            # Step 2: Scrape each commodity
            all_records = []
            success_count = 0
            error_count = 0
            
            for code, name in commodities.items():
                logger.info(f"📡 Fetching {name} ({code})...")
                
                try:
                    raw_data = self.fetch_commodity_data(code, name)
                    if not raw_data:
                        error_count += 1
                        continue
                    
                    records = self.parse_data(raw_data, code, name)
                    if records:
                        all_records.extend(records)
                        success_count += 1
                        logger.info(f"✔ {len(records)} rows for {name}")
                    
                    time.sleep(1.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {name}: {e}")
                    error_count += 1
            
            # Step 3: Save to database
            if all_records:
                upserted = self.db.upsert_market_prices(all_records)
                self.db.update_source_status(self.config['id'], 'success')
                
                logger.info(f"✅ {self.organization} COMPLETE: {upserted} records, {success_count} success, {error_count} errors")
                return True
            else:
                logger.error(f"❌ No data collected for {self.organization}")
                self.db.update_source_status(self.config['id'], 'failed', 'No data collected')
                return False
                
        except Exception as e:
            logger.error(f"❌ Fatal error in {self.organization}: {e}", exc_info=True)
            self.db.update_source_status(self.config['id'], 'failed', str(e))
            return False


# -----------------------------------
# Maharashtra (MSAMB) Scraper
# -----------------------------------
class MSAMBScraper(BaseAPMCScraper):
    """Maharashtra State Agricultural Marketing Board scraper"""
    
    def fetch_commodities(self) -> Dict[str, str]:
        """Load commodities from saved HTML dropdown"""
        html_path = self.config.get('commodity_html_path', './data/msamb_commodities.html')
        
        if not os.path.exists(html_path):
            logger.error(f"❌ Commodity HTML not found: {html_path}")
            return {}
        
        with open(html_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'lxml')
        
        select = soup.find('select', {'id': 'drpCommodities'})
        if not select:
            return {}
        
        commodities = {}
        for opt in select.find_all('option'):
            code = opt.get('value', '').strip()
            name = opt.text.strip()
            if code and code.isdigit() and len(code) == 5:
                commodities[code] = name
        
        return commodities
    
    def fetch_commodity_data(self, commodity_code: str, commodity_name: str) -> Optional[str]:
        """Fetch data via GET request"""
        # Establish session if needed
        if not self.session.cookies:
            main_page = self.config.get('main_page')
            self.session.get(main_page, timeout=20)
        
        endpoint = self.config.get('data_endpoint')
        params = {'commodityCode': commodity_code, 'apmcCode': 'null'}
        
        try:
            r = self.session.get(endpoint, params=params, timeout=30)
            if r.status_code == 200 and '<tr' in r.text:
                return r.text
        except Exception as e:
            logger.error(f"Error fetching {commodity_code}: {e}")
        
        return None
    
    def parse_data(self, html: str, commodity_code: str, commodity_name: str) -> List[Dict]:
        """Parse MSAMB HTML table structure"""
        soup = BeautifulSoup(html, 'html.parser')
        records = []
        current_date = None
        
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            
            # Date row
            if len(tds) == 1 or (len(tds) >= 7 and 'colspan' in tds[0].attrs):
                date_str = tds[0].get_text(strip=True)
                try:
                    current_date = datetime.datetime.strptime(date_str, '%d/%m/%Y').date()
                except:
                    current_date = datetime.date.today()
                continue
            
            # Data row
            if len(tds) >= 7 and current_date:
                normalized = self.normalize_commodity(commodity_name)
                arrival = self.clean_numeric(tds[3].get_text(strip=True))
                min_price = self.clean_numeric(tds[4].get_text(strip=True))
                max_price = self.clean_numeric(tds[5].get_text(strip=True))
                modal_price = self.clean_numeric(tds[6].get_text(strip=True))
                
                record = {
                    'source_id': self.config['id'],
                    'commodity_code': commodity_code,
                    'crop_name': commodity_name,
                    'variety': tds[1].get_text(strip=True) or None,
                    'unit': tds[2].get_text(strip=True) or 'quintal',
                    'market_location': tds[0].get_text(strip=True),
                    'district': None,
                    'state': self.state_code,
                    'price_date': current_date.isoformat(),
                    'arrival': arrival,
                    'min_price': min_price,
                    'max_price': max_price,
                    'modal_price': modal_price,
                    'price_per_unit': modal_price or max_price,
                    'price_type': 'wholesale',
                    'spread': (max_price - min_price) if (min_price and max_price) else None,
                    'global_commodity_code': normalized['global_code'],
                    'commodity_name_normalized': normalized['name'],
                    'commodity_category': normalized['category'],
                    'metadata': {'source': self.organization},
                    'source': 'apmc_scraper',
                    'fetched_at': datetime.datetime.now().isoformat(),
                    'ingested_at': datetime.datetime.now().isoformat(),
                    'status': 'ready'
                }
                records.append(record)
        
        return records


# -----------------------------------
# Karnataka (RAITHAMITRA) Scraper - Example Template
# -----------------------------------
class KarnatakaScraper(BaseAPMCScraper):
    """Karnataka APMC scraper - Different structure than Maharashtra"""
    
    def fetch_commodities(self) -> Dict[str, str]:
        """Karnataka: Fetch commodities via API or different method"""
        # TODO: Implement Karnataka-specific logic
        # Example: They might use a JSON API instead of HTML dropdown
        logger.warning("⚠️ Karnataka scraper not fully implemented yet")
        return {}
    
    def fetch_commodity_data(self, commodity_code: str, commodity_name: str) -> Optional[str]:
        """Karnataka: Different data fetching method"""
        # TODO: Implement Karnataka-specific logic
        return None
    
    def parse_data(self, raw_data: str, commodity_code: str, commodity_name: str) -> List[Dict]:
        """Karnataka: Different HTML/JSON structure"""
        # TODO: Implement Karnataka-specific parsing
        return []


# -----------------------------------
# Scraper Factory
# -----------------------------------
class ScraperFactory:
    """Factory to create appropriate scraper based on state/organization"""
    
    SCRAPER_MAP: Dict[str, Type[BaseAPMCScraper]] = {
        'MSAMB': MSAMBScraper,
        'MH': MSAMBScraper,  # Maharashtra
        'KARNATAKA_APMC': KarnatakaScraper,
        'KA': KarnatakaScraper,
        # Add more states here:
        # 'UP': UttarPradeshScraper,
        # 'GJ': GujaratScraper,
        # 'PB': PunjabScraper,
    }
    
    @classmethod
    def create_scraper(cls, db_manager: SupabaseManager, config: Dict) -> Optional[BaseAPMCScraper]:
        """Create appropriate scraper based on configuration"""
        organization = config.get('organization')
        state_code = config.get('state_code')
        
        # Try organization first, then state code
        scraper_class = cls.SCRAPER_MAP.get(organization) or cls.SCRAPER_MAP.get(state_code)
        
        if scraper_class:
            logger.info(f"✅ Creating {scraper_class.__name__} for {organization}")
            return scraper_class(db_manager, config)
        else:
            logger.error(f"❌ No scraper found for {organization} ({state_code})")
            return None


# -----------------------------------
# Main Orchestrator
# -----------------------------------
def main():
    """Main execution - scrapes all active sources"""
    logger.info("=" * 70)
    logger.info("🚀 STARTING MULTI-STATE APMC SCRAPER")
    logger.info("=" * 70)
    
    try:
        # Initialize Supabase
        db = SupabaseManager(SUPABASE_URL, SUPABASE_KEY)
        
        # Get all active sources (or filter by state if needed)
        state_filter = os.getenv('STATE_CODE')  # Optional: scrape only specific state
        sources = db.get_active_sources(state_filter)
        
        if not sources:
            logger.error("❌ No active sources found in database")
            sys.exit(1)
        
        logger.info(f"📋 Found {len(sources)} active source(s) to scrape")
        
        # Scrape each source
        results = []
        for source in sources:
            logger.info("")
            logger.info("=" * 70)
            logger.info(f"Processing: {source['organization']} ({source['state_code']})")
            logger.info("=" * 70)
            
            # Create appropriate scraper
            scraper = ScraperFactory.create_scraper(db, source)
            
            if not scraper:
                logger.error(f"⚠️ Skipping {source['organization']} - no scraper implementation")
                results.append({'source': source['organization'], 'status': 'skipped', 'reason': 'No scraper'})
                continue
            
            # Run scraper
            success = scraper.run()
            results.append({
                'source': source['organization'],
                'state': source['state_code'],
                'status': 'success' if success else 'failed'
            })
        
        # Summary report
        logger.info("")
        logger.info("=" * 70)
        logger.info("📊 SCRAPING SUMMARY")
        logger.info("=" * 70)
        
        for result in results:
            status_emoji = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"{status_emoji} {result['source']} ({result.get('state', 'N/A')}): {result['status']}")
        
        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info("")
        logger.info(f"Total: {len(results)} sources | Success: {success_count} | Failed: {len(results) - success_count}")
        logger.info("=" * 70)
        
        # Exit with error if all failed
        if success_count == 0:
            logger.error("❌ All scrapers failed!")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
