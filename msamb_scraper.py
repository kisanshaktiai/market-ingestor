"""
Universal Multi-State APMC Market Price Scraper
Author: Amarsinh Patil (KisanShaktiAI)

Architecture:
- Database-driven configuration for each state's scraper
- Plugin-based scraper system for different website structures
- Unified data storage in Supabase
- Supports multiple states with different scraping logic
"""

import os
import sys
import time
import logging
import datetime
import importlib
from typing import Dict, List, Optional, Type
from abc import ABC, abstractmethod
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('apmc_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Supabase configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)


# ===================================================================
# BASE SCRAPER INTERFACE (Abstract Class)
# ===================================================================
class BaseAPMCScraper(ABC):
    """
    Abstract base class that all state-specific scrapers must implement.
    This ensures consistent interface across all state scrapers.
    """
    
    def __init__(self, supabase: Client, config: Dict):
        self.supabase = supabase
        self.config = config
        self.state_code = config.get('state_code')
        self.organization = config.get('organization')
        self.source_id = config.get('id')
        
        logger.info(f"Initialized {self.organization} scraper for {self.state_code}")
    
    @abstractmethod
    def fetch_commodities(self) -> Dict[str, str]:
        """
        Fetch available commodities from the state's website.
        Returns: Dict[commodity_code, commodity_name]
        """
        pass
    
    @abstractmethod
    def fetch_commodity_prices(self, commodity_code: str, commodity_name: str) -> List[Dict]:
        """
        Fetch market prices for a specific commodity.
        Returns: List of standardized price records
        """
        pass
    
    @abstractmethod
    def parse_date(self, date_string: str) -> datetime.date:
        """
        Parse date string according to state's date format.
        Returns: datetime.date object
        """
        pass
    
    def normalize_record(self, raw_record: Dict) -> Dict:
        """
        Convert state-specific raw data to standardized format.
        Override this if needed for state-specific normalization.
        """
        return {
            'source_id': self.source_id,
            'commodity_code': raw_record.get('commodity_code'),
            'crop_name': raw_record.get('crop_name'),
            'variety': raw_record.get('variety'),
            'unit': raw_record.get('unit', 'quintal'),
            'market_location': raw_record.get('market_location'),
            'district': raw_record.get('district'),
            'state': self.state_code,
            'price_date': raw_record.get('price_date'),
            'arrival': raw_record.get('arrival'),
            'min_price': raw_record.get('min_price'),
            'max_price': raw_record.get('max_price'),
            'modal_price': raw_record.get('modal_price'),
            'price_per_unit': raw_record.get('modal_price') or raw_record.get('max_price'),
            'price_type': 'wholesale',
            'spread': self._calculate_spread(raw_record),
            'metadata': raw_record.get('metadata', {}),
            'source': f"{self.organization.lower()}_scraper",
            'fetched_at': datetime.datetime.now().isoformat(),
            'ingested_at': datetime.datetime.now().isoformat(),
            'status': 'ready'
        }
    
    def _calculate_spread(self, record: Dict) -> Optional[float]:
        """Calculate price spread"""
        min_p = record.get('min_price')
        max_p = record.get('max_price')
        if min_p and max_p:
            return max_p - min_p
        return None
    
    def run(self) -> Dict:
        """
        Execute the scraping process.
        This is the main entry point for all scrapers.
        """
        logger.info(f"=" * 70)
        logger.info(f"Starting scraper: {self.organization} ({self.state_code})")
        logger.info(f"=" * 70)
        
        stats = {
            'organization': self.organization,
            'state': self.state_code,
            'start_time': datetime.datetime.now(),
            'commodities_fetched': 0,
            'records_scraped': 0,
            'records_upserted': 0,
            'errors': 0,
            'success': False
        }
        
        try:
            # Step 1: Fetch commodities
            logger.info("Step 1: Fetching commodity list...")
            commodities = self.fetch_commodities()
            stats['commodities_fetched'] = len(commodities)
            logger.info(f"✅ Found {len(commodities)} commodities")
            
            if not commodities:
                logger.error("❌ No commodities found")
                return stats
            
            # Step 2: Scrape each commodity
            logger.info("Step 2: Scraping market prices...")
            all_records = []
            
            for code, name in commodities.items():
                try:
                    logger.info(f"  📡 Fetching: {name} ({code})")
                    records = self.fetch_commodity_prices(code, name)
                    
                    if records:
                        # Normalize records
                        normalized = [self.normalize_record(r) for r in records]
                        all_records.extend(normalized)
                        logger.info(f"     ✅ {len(records)} records")
                    else:
                        logger.warning(f"     ⚠️  No data")
                    
                    # Rate limiting
                    time.sleep(self.config.get('rate_limit_delay', 1.5))
                    
                except Exception as e:
                    logger.error(f"     ❌ Error: {e}")
                    stats['errors'] += 1
                    continue
            
            stats['records_scraped'] = len(all_records)
            
            # Step 3: Upsert to Supabase
            if all_records:
                logger.info(f"Step 3: Upserting {len(all_records)} records to Supabase...")
                upserted = self._upsert_records(all_records)
                stats['records_upserted'] = upserted
                stats['success'] = True
            else:
                logger.warning("⚠️  No records to upsert")
            
            # Step 4: Update last checked time
            self._update_source_status(stats)
            
        except Exception as e:
            logger.error(f"❌ Fatal error in scraper: {e}", exc_info=True)
            stats['error_message'] = str(e)
        
        finally:
            stats['end_time'] = datetime.datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            self._log_summary(stats)
        
        return stats
    
    def _upsert_records(self, records: List[Dict], batch_size: int = 100) -> int:
        """Upsert records to Supabase in batches"""
        total_upserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                self.supabase.table('market_prices').upsert(
                    batch,
                    on_conflict='source_id,commodity_code,price_date,market_location'
                ).execute()
                total_upserted += len(batch)
                logger.info(f"  ✅ Batch {i//batch_size + 1}: {len(batch)} records")
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"  ❌ Batch {i//batch_size + 1} failed: {e}")
        
        return total_upserted
    
    def _update_source_status(self, stats: Dict):
        """Update source metadata"""
        try:
            self.supabase.table('agri_market_sources').update({
                'last_checked_at': datetime.datetime.now().isoformat(),
                'metadata': {
                    'last_run': stats,
                    'last_success': stats['success']
                }
            }).eq('id', self.source_id).execute()
        except Exception as e:
            logger.warning(f"⚠️  Could not update source status: {e}")
    
    def _log_summary(self, stats: Dict):
        """Log execution summary"""
        logger.info("=" * 70)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Organization: {stats['organization']}")
        logger.info(f"State: {stats['state']}")
        logger.info(f"Duration: {stats['duration']:.2f}s")
        logger.info(f"Commodities: {stats['commodities_fetched']}")
        logger.info(f"Records Scraped: {stats['records_scraped']}")
        logger.info(f"Records Upserted: {stats['records_upserted']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Status: {'✅ SUCCESS' if stats['success'] else '❌ FAILED'}")
        logger.info("=" * 70)


# ===================================================================
# MAHARASHTRA (MSAMB) SCRAPER IMPLEMENTATION
# ===================================================================
class MSAMBScraper(BaseAPMCScraper):
    """Maharashtra State Agricultural Marketing Board scraper"""
    
    def __init__(self, supabase: Client, config: Dict):
        super().__init__(supabase, config)
        import requests
        from bs4 import BeautifulSoup
        
        self.requests = requests
        self.BeautifulSoup = BeautifulSoup
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        
        # Establish session
        if config.get('page_requires_session'):
            self._establish_session()
    
    def _establish_session(self):
        """Establish session with MSAMB website"""
        try:
            main_page = self.config.get('main_page')
            r = self.session.get(main_page, timeout=20)
            r.raise_for_status()
            logger.info(f"✅ Session established with {self.organization}")
        except Exception as e:
            logger.error(f"❌ Session failed: {e}")
            raise
    
    def fetch_commodities(self) -> Dict[str, str]:
        """Fetch commodities from HTML dropdown file"""
        html_path = self.config.get('commodity_html_path')
        
        with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
            html = f.read()
        
        soup = self.BeautifulSoup(html, 'lxml')
        select_tag = soup.find('select', {'id': 'drpCommodities'})
        
        commodities = {}
        for opt in select_tag.find_all('option'):
            code = opt.get('value', '').strip()
            name = opt.text.strip()
            if code and code.isdigit() and len(code) == 5:
                commodities[code] = name
        
        return commodities
    
    def fetch_commodity_prices(self, commodity_code: str, commodity_name: str) -> List[Dict]:
        """Fetch prices for one commodity from MSAMB"""
        try:
            data_endpoint = self.config.get('data_endpoint')
            params = {
                'commodityCode': commodity_code,
                'apmcCode': 'null'
            }
            
            r = self.session.get(data_endpoint, params=params, timeout=30)
            if r.status_code != 200:
                return []
            
            return self._parse_msamb_html(r.text, commodity_code, commodity_name)
            
        except Exception as e:
            logger.error(f"Error fetching {commodity_code}: {e}")
            return []
    
    def _parse_msamb_html(self, html: str, code: str, name: str) -> List[Dict]:
        """Parse MSAMB HTML table structure"""
        soup = self.BeautifulSoup(html, 'html.parser')
        records = []
        current_date = None
        
        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            
            # Date row (colspan)
            if len(tds) == 1 or (len(tds) >= 7 and 'colspan' in tds[0].attrs):
                date_text = tds[0].get_text(strip=True)
                if date_text:
                    current_date = self.parse_date(date_text)
                continue
            
            # Data row
            if len(tds) >= 7 and current_date:
                try:
                    records.append({
                        'commodity_code': code,
                        'crop_name': name,
                        'market_location': tds[0].get_text(strip=True),
                        'variety': tds[1].get_text(strip=True) or None,
                        'unit': tds[2].get_text(strip=True) or 'quintal',
                        'arrival': self._clean_numeric(tds[3].get_text(strip=True)),
                        'min_price': self._clean_numeric(tds[4].get_text(strip=True)),
                        'max_price': self._clean_numeric(tds[5].get_text(strip=True)),
                        'modal_price': self._clean_numeric(tds[6].get_text(strip=True)),
                        'price_date': current_date.isoformat(),
                        'district': None,
                        'metadata': {
                            'source_website': 'msamb.com',
                            'raw_commodity': name
                        }
                    })
                except Exception as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue
        
        return records
    
    def parse_date(self, date_string: str) -> datetime.date:
        """Parse DD/MM/YYYY format"""
        try:
            return datetime.datetime.strptime(date_string, '%d/%m/%Y').date()
        except:
            return datetime.date.today()
    
    def _clean_numeric(self, value: str) -> Optional[float]:
        """Clean numeric values"""
        if not value or value == '-':
            return None
        try:
            import re
            cleaned = re.sub(r'[^0-9.]', '', value)
            return float(cleaned) if cleaned else None
        except:
            return None


# ===================================================================
# KARNATAKA (APMC) SCRAPER IMPLEMENTATION EXAMPLE
# ===================================================================
class KarnatakaAPMCScraper(BaseAPMCScraper):
    """Karnataka APMC scraper - different website structure"""
    
    def fetch_commodities(self) -> Dict[str, str]:
        """Karnataka-specific commodity fetching logic"""
        # Implement Karnataka's API/scraping logic here
        # This might use JSON API, different HTML structure, etc.
        pass
    
    def fetch_commodity_prices(self, commodity_code: str, commodity_name: str) -> List[Dict]:
        """Karnataka-specific price fetching logic"""
        pass
    
    def parse_date(self, date_string: str) -> datetime.date:
        """Karnataka-specific date format"""
        pass


# ===================================================================
# SCRAPER FACTORY - Dynamically loads state scrapers
# ===================================================================
class ScraperFactory:
    """Factory to create appropriate scraper based on state configuration"""
    
    # Registry of available scrapers
    SCRAPERS = {
        'MSAMB': MSAMBScraper,
        'KARNATAKA_APMC': KarnatakaAPMCScraper,
        # Add more state scrapers here as you build them
    }
    
    @classmethod
    def create_scraper(cls, supabase: Client, config: Dict) -> BaseAPMCScraper:
        """Create scraper instance based on organization"""
        organization = config.get('organization')
        
        scraper_class = cls.SCRAPERS.get(organization)
        if not scraper_class:
            raise ValueError(f"No scraper found for organization: {organization}")
        
        return scraper_class(supabase, config)
    
    @classmethod
    def register_scraper(cls, organization: str, scraper_class: Type[BaseAPMCScraper]):
        """Register a new scraper dynamically"""
        cls.SCRAPERS[organization] = scraper_class
        logger.info(f"Registered scraper: {organization}")


# ===================================================================
# ORCHESTRATOR - Manages multi-state scraping
# ===================================================================
class APMCOrchestrator:
    """Orchestrates scraping across multiple states"""
    
    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("✅ Supabase client initialized")
    
    def get_active_sources(self, state_filter: Optional[str] = None) -> List[Dict]:
        """Get all active scraper configurations"""
        try:
            query = self.supabase.table('agri_market_sources').select('*').eq('active', True)
            
            if state_filter:
                query = query.eq('state_code', state_filter)
            
            response = query.execute()
            return response.data
        except Exception as e:
            logger.error(f"Error fetching sources: {e}")
            return []
    
    def run_all_scrapers(self, state_filter: Optional[str] = None):
        """Run all active scrapers"""
        logger.info("🚀 Starting APMC Orchestrator")
        logger.info("=" * 70)
        
        sources = self.get_active_sources(state_filter)
        logger.info(f"Found {len(sources)} active sources")
        
        if not sources:
            logger.warning("No active sources found")
            return
        
        results = []
        
        for source in sources:
            try:
                logger.info(f"\n{'='*70}")
                logger.info(f"Processing: {source['organization']} ({source['state_code']})")
                logger.info(f"{'='*70}")
                
                # Create appropriate scraper
                scraper = ScraperFactory.create_scraper(self.supabase, source)
                
                # Run scraper
                stats = scraper.run()
                results.append(stats)
                
            except Exception as e:
                logger.error(f"❌ Failed to process {source['organization']}: {e}")
                continue
        
        # Summary
        self._log_orchestrator_summary(results)
        
        return results
    
    def run_single_scraper(self, organization: str):
        """Run scraper for a specific organization"""
        logger.info(f"🎯 Running single scraper: {organization}")
        
        try:
            response = self.supabase.table('agri_market_sources') \
                .select('*') \
                .eq('organization', organization) \
                .eq('active', True) \
                .limit(1) \
                .execute()
            
            if not response.data:
                logger.error(f"No active source found for {organization}")
                return None
            
            source = response.data[0]
            scraper = ScraperFactory.create_scraper(self.supabase, source)
            return scraper.run()
            
        except Exception as e:
            logger.error(f"Error running {organization} scraper: {e}")
            return None
    
    def _log_orchestrator_summary(self, results: List[Dict]):
        """Log summary of all scrapers"""
        logger.info("\n" + "=" * 70)
        logger.info("ORCHESTRATOR SUMMARY")
        logger.info("=" * 70)
        
        total_records = sum(r.get('records_upserted', 0) for r in results)
        successful = sum(1 for r in results if r.get('success'))
        failed = len(results) - successful
        
        logger.info(f"Total Sources: {len(results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total Records: {total_records}")
        logger.info("=" * 70)


# ===================================================================
# MAIN ENTRY POINT
# ===================================================================
def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-State APMC Scraper')
    parser.add_argument('--state', help='Filter by state code (e.g., MH, KA)')
    parser.add_argument('--org', help='Run specific organization (e.g., MSAMB)')
    args = parser.parse_args()
    
    try:
        orchestrator = APMCOrchestrator()
        
        if args.org:
            # Run single scraper
            orchestrator.run_single_scraper(args.org)
        else:
            # Run all scrapers (optionally filtered by state)
            orchestrator.run_all_scrapers(state_filter=args.state)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
