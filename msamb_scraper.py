"""
MSAMB Market Price Scraper (Supabase Cloud Integration)
Author: Amarsinh Patil (KisanShaktiAI)
Enhanced: Supabase cloud database integration with upsert operations
"""

import os
import sys
import time
import logging
import datetime
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import json

# Supabase integration
from supabase import create_client, Client

# -----------------------------------
# Configuration & Logging
# -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('msamb_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Supabase configuration from environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')  # Use service role key for server-side operations

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment variables")
    sys.exit(1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


# -----------------------------------
# Supabase Database Manager
# -----------------------------------
class SupabaseManager:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)
        logger.info("✅ Supabase client initialized")
        
    def get_source_config(self, organization: str = "MSAMB") -> Optional[Dict]:
        """Fetch scraper configuration from agri_market_sources"""
        try:
            response = self.supabase.table('agri_market_sources') \
                .select('*') \
                .eq('organization', organization) \
                .eq('active', True) \
                .limit(1) \
                .execute()
            
            if response.data and len(response.data) > 0:
                logger.info(f"✅ Loaded config for {organization}")
                return response.data[0]
            else:
                logger.error(f"❌ No active config found for {organization}")
                return None
        except Exception as e:
            logger.error(f"❌ Error fetching source config: {e}")
            return None
    
    def get_commodity_mapping(self) -> Dict[str, Dict]:
        """Load commodity master for normalization"""
        try:
            response = self.supabase.table('commodity_master') \
                .select('global_code, name, aliases, unit, category') \
                .execute()
            
            # Create mapping dict with aliases
            mapping = {}
            for row in response.data:
                mapping[row['name'].lower()] = row
                if row.get('aliases'):
                    aliases = row['aliases'] if isinstance(row['aliases'], list) else []
                    for alias in aliases:
                        mapping[alias.lower()] = row
            
            logger.info(f"✅ Loaded {len(response.data)} commodities from master")
            return mapping
        except Exception as e:
            logger.error(f"❌ Error loading commodity mapping: {e}")
            return {}
    
    def upsert_market_prices(self, records: List[Dict], batch_size: int = 100) -> int:
        """
        Bulk upsert market prices to Supabase
        Uses batch processing to handle large datasets
        """
        if not records:
            return 0
        
        total_upserted = 0
        
        try:
            # Process in batches to avoid payload size limits
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Supabase upsert (conflict on unique constraint)
                response = self.supabase.table('market_prices') \
                    .upsert(
                        batch,
                        on_conflict='source_id,commodity_code,price_date,market_location',
                        returning='minimal'
                    ) \
                    .execute()
                
                total_upserted += len(batch)
                logger.info(f"✔ Upserted batch {i//batch_size + 1}: {len(batch)} records")
                
                # Small delay between batches to avoid rate limiting
                if i + batch_size < len(records):
                    time.sleep(0.5)
            
            logger.info(f"✅ Total upserted: {total_upserted} records")
            return total_upserted
            
        except Exception as e:
            logger.error(f"❌ Error upserting market prices: {e}")
            raise
    
    def update_source_check_time(self, source_id: str):
        """Update last_checked_at for source"""
        try:
            self.supabase.table('agri_market_sources') \
                .update({'last_checked_at': datetime.datetime.now().isoformat()}) \
                .eq('id', source_id) \
                .execute()
            logger.info(f"✅ Updated last_checked_at for source {source_id}")
        except Exception as e:
            logger.error(f"⚠️ Error updating source check time: {e}")
    
    def get_state_id(self, state_code: str) -> Optional[str]:
        """Get state ID from state code"""
        try:
            response = self.supabase.table('states') \
                .select('id') \
                .eq('code', state_code) \
                .limit(1) \
                .execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0]['id']
            return None
        except Exception as e:
            logger.error(f"⚠️ Error fetching state ID: {e}")
            return None
    
    def get_country_id(self, country_code: str = 'IN') -> Optional[str]:
        """Get country ID from country code"""
        try:
            response = self.supabase.table('countries') \
                .select('id') \
                .eq('code', country_code) \
                .limit(1) \
                .execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0]['id']
            return None
        except Exception as e:
            logger.error(f"⚠️ Error fetching country ID: {e}")
            return None


# -----------------------------------
# Scraper Logic
# -----------------------------------
class MSAMBScraper:
    def __init__(self, supabase_manager: SupabaseManager, source_config: Dict):
        self.db = supabase_manager
        self.config = source_config
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.commodity_mapping = supabase_manager.get_commodity_mapping()
        
        # Get reference IDs
        self.state_id = supabase_manager.get_state_id(source_config.get('state_code', 'MH'))
        self.country_id = supabase_manager.get_country_id('IN')
        
    def load_commodities_from_html(self, file_path: str) -> Dict[str, str]:
        """Parse commodities from saved HTML dropdown"""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            
            soup = BeautifulSoup(html, "lxml")
            selector = self.config.get('commodity_dropdown_selector', 'select#drpCommodities')
            select_tag = soup.select_one(selector) or soup.find("select")
            
            if not select_tag:
                raise Exception(f"❌ No <select> tag found using selector: {selector}")
            
            commodities = {}
            value_attr = self.config.get('commodity_value_attr', 'value')
            
            for opt in select_tag.find_all("option"):
                code = opt.get(value_attr, "").strip()
                name = opt.text.strip()
                if code and code.isdigit() and len(code) == 5:
                    commodities[code] = name
            
            logger.info(f"✅ Loaded {len(commodities)} commodities from dropdown HTML")
            return commodities
            
        except Exception as e:
            logger.error(f"❌ Error loading commodities: {e}")
            return {}
    
    def establish_session(self) -> bool:
        """Establish session with main page"""
        try:
            main_page = self.config.get('main_page')
            if not main_page:
                logger.error("❌ main_page not configured")
                return False
            
            logger.info(f"🚀 Opening main page: {main_page}")
            r = self.session.get(main_page, allow_redirects=True, timeout=20)
            r.raise_for_status()
            
            cookies = self.session.cookies.get_dict()
            logger.info(f"✅ Session established with cookies: {list(cookies.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Session establishment failed: {e}")
            return False
    
    def fetch_commodity_data(self, commodity_code: str) -> Optional[str]:
        """Fetch HTML data for one commodity"""
        try:
            data_endpoint = self.config.get('data_endpoint')
            request_params = self.config.get('data_request_params', {})
            
            # Replace placeholders in params
            params = {}
            for key, value in request_params.items():
                if value == "{commodity_code}":
                    params[key] = commodity_code
                else:
                    params[key] = value
            
            method = self.config.get('data_request_method', 'GET').upper()
            
            if method == 'GET':
                r = self.session.get(data_endpoint, params=params, timeout=30)
            else:
                r = self.session.post(data_endpoint, data=params, timeout=30)
            
            if r.status_code == 200 and "<tr" in r.text:
                return r.text
            else:
                logger.warning(f"⚠️ Empty response for {commodity_code} ({r.status_code})")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error fetching {commodity_code}: {e}")
            return None
    
    def parse_html_table(self, html: str, commodity_code: str, commodity_name: str) -> List[Dict]:
        """Parse HTML table into structured data for Supabase"""
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        current_date = None
        
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            
            # Detect date row
            if len(tds) == 1 or (len(tds) >= 7 and "colspan" in tds[0].attrs):
                date_text = tds[0].get_text(strip=True)
                if date_text:
                    current_date = self.parse_date(date_text)
                continue
            
            # Data row
            if len(tds) >= 7 and current_date:
                try:
                    raw_data = {
                        'market': tds[0].get_text(strip=True),
                        'variety': tds[1].get_text(strip=True),
                        'unit': tds[2].get_text(strip=True),
                        'arrival': self.clean_numeric(tds[3].get_text(strip=True)),
                        'min_price': self.clean_numeric(tds[4].get_text(strip=True)),
                        'max_price': self.clean_numeric(tds[5].get_text(strip=True)),
                        'modal_price': self.clean_numeric(tds[6].get_text(strip=True)),
                    }
                    
                    # Normalize commodity
                    normalized = self.normalize_commodity(commodity_name)
                    
                    # Calculate spread and price_per_unit
                    spread = None
                    if raw_data['min_price'] and raw_data['max_price']:
                        spread = raw_data['max_price'] - raw_data['min_price']
                    
                    price_per_unit = raw_data['modal_price'] or raw_data['max_price']
                    
                    # Prepare record for Supabase (all fields must match table schema)
                    record = {
                        'source_id': self.config['id'],
                        'commodity_code': commodity_code,
                        'crop_name': commodity_name,
                        'variety': raw_data['variety'] or None,
                        'unit': raw_data['unit'] or 'quintal',
                        'market_location': raw_data['market'],
                        'district': None,
                        'state': self.config.get('state_code', 'MH'),
                        'price_date': current_date.isoformat(),
                        'arrival': raw_data['arrival'],
                        'min_price': raw_data['min_price'],
                        'max_price': raw_data['max_price'],
                        'modal_price': raw_data['modal_price'],
                        'price_per_unit': price_per_unit,
                        'price_type': 'wholesale',
                        'spread': spread,
                        'global_commodity_code': normalized['global_code'],
                        'commodity_name_normalized': normalized['name'],
                        'commodity_category': normalized['category'],
                        'metadata': {
                            'source': 'MSAMB',
                            'raw_commodity_name': commodity_name,
                            'scrape_timestamp': datetime.datetime.now().isoformat()
                        },
                        'source': 'msamb_scraper',
                        'state_id': self.state_id,
                        'country_id': self.country_id,
                        'fetched_at': datetime.datetime.now().isoformat(),
                        'ingested_at': datetime.datetime.now().isoformat(),
                        'status': 'ready'
                    }
                    
                    rows.append(record)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing row: {e}")
                    continue
        
        return rows
    
    def parse_date(self, date_str: str) -> datetime.date:
        """Parse date string to date object"""
        try:
            # Common Indian date format: DD/MM/YYYY
            date_format = self.config.get('date_format', '%d/%m/%Y')
            return datetime.datetime.strptime(date_str, date_format).date()
        except Exception as e:
            logger.warning(f"⚠️ Error parsing date '{date_str}': {e}")
            return datetime.date.today()
    
    def clean_numeric(self, value: str) -> Optional[float]:
        """Clean and convert numeric string"""
        if not value or value == '-':
            return None
        try:
            import re
            cleaned = re.sub(r'[^0-9.]', '', value)
            return float(cleaned) if cleaned else None
        except:
            return None
    
    def normalize_commodity(self, commodity_name: str) -> Dict:
        """Normalize commodity name using commodity_master"""
        key = commodity_name.lower().strip()
        
        if key in self.commodity_mapping:
            mapped = self.commodity_mapping[key]
            return {
                'global_code': mapped['global_code'],
                'name': mapped['name'],
                'category': mapped['category']
            }
        else:
            logger.debug(f"⚠️ Commodity not mapped: {commodity_name}")
            return {
                'global_code': None,
                'name': commodity_name,
                'category': None
            }
    
    def run(self):
        """Main scraper execution"""
        logger.info("=" * 60)
        logger.info("🚀 Starting MSAMB Market Price Scraper (Supabase Cloud)")
        logger.info("=" * 60)
        
        # Load commodities
        html_path = self.config.get('commodity_html_path')
        if not html_path or not os.path.exists(html_path):
            logger.error(f"❌ Commodity HTML file not found: {html_path}")
            return
        
        commodities = self.load_commodities_from_html(html_path)
        if not commodities:
            logger.error("❌ No commodities loaded")
            return
        
        # Establish session
        if not self.establish_session():
            logger.error("❌ Failed to establish session")
            return
        
        # Fetch data for each commodity
        all_records = []
        success_count = 0
        error_count = 0
        
        for code, name in commodities.items():
            logger.info(f"📡 Fetching data for {name} ({code})...")
            
            html = self.fetch_commodity_data(code)
            if not html:
                error_count += 1
                continue
            
            records = self.parse_html_table(html, code, name)
            if records:
                all_records.extend(records)
                success_count += 1
                logger.info(f"✔ {len(records)} rows found for {name}")
            else:
                logger.warning(f"⚠️ No rows found for {name}")
            
            # Rate limiting
            time.sleep(1.5)
        
        # Upsert to Supabase
        if all_records:
            try:
                upserted = self.db.upsert_market_prices(all_records, batch_size=100)
                self.db.update_source_check_time(self.config['id'])
                
                logger.info("=" * 60)
                logger.info(f"✅ SCRAPING COMPLETE")
                logger.info(f"   Total commodities: {len(commodities)}")
                logger.info(f"   Successful: {success_count}")
                logger.info(f"   Errors: {error_count}")
                logger.info(f"   Records upserted: {upserted}")
                logger.info("=" * 60)
                
            except Exception as e:
                logger.error(f"❌ Error during Supabase upsert: {e}")
                raise
        else:
            logger.error("❌ No data collected")


# -----------------------------------
# Main Execution
# -----------------------------------
def main():
    try:
        # Initialize Supabase manager
        supabase_manager = SupabaseManager(SUPABASE_URL, SUPABASE_KEY)
        
        # Get source configuration
        source_config = supabase_manager.get_source_config("MSAMB")
        if not source_config:
            logger.error("❌ No active source configuration found for MSAMB")
            sys.exit(1)
        
        # Run scraper
        scraper = MSAMBScraper(supabase_manager, source_config)
        scraper.run()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()