"""
Universal Multi-State APMC Market Price Scraper
Author: Amarsinh Patil (KisanShaktiAI)
"""

import os
import sys
import time
import logging
import datetime
from typing import Dict, List, Optional, Type
from abc import ABC, abstractmethod
from urllib.parse import urljoin

from supabase import create_client, Client

# ============================================================
# URL BUILDER (matches local hardcoded logic)
# ============================================================
def build_url(base_url: str, path_or_url: str) -> str:
    if not path_or_url:
        raise ValueError("URL missing in config")

    if path_or_url.startswith("http"):
        return path_or_url

    return urljoin(base_url.rstrip("/") + "/", path_or_url.lstrip("/"))


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("apmc_scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================
# SUPABASE CONFIG
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)


# ============================================================
# BASE SCRAPER
# ============================================================
class BaseAPMCScraper(ABC):
    def __init__(self, supabase: Client, config: Dict):
        self.supabase = supabase
        self.config = config
        self.organization = config.get("organization")
        self.state_code = config.get("state_code")
        self.source_id = config.get("id")

        logger.info(f"Initialized {self.organization} scraper for {self.state_code}")

    @abstractmethod
    def fetch_commodities(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def fetch_commodity_prices(self, commodity_code: str, commodity_name: str) -> List[Dict]:
        pass

    @abstractmethod
    def parse_date(self, date_string: str) -> datetime.date:
        pass

    def normalize_record(self, raw: Dict) -> Dict:
        return {
            "source_id": self.source_id,
            "commodity_code": raw.get("commodity_code"),
            "crop_name": raw.get("crop_name"),
            "variety": raw.get("variety"),
            "unit": raw.get("unit", "quintal"),
            "market_location": raw.get("market_location"),
            "district": raw.get("district"),
            "state": self.state_code,
            "price_date": raw.get("price_date"),
            "arrival": raw.get("arrival"),
            "min_price": raw.get("min_price"),
            "max_price": raw.get("max_price"),
            "modal_price": raw.get("modal_price"),
            "price_per_unit": raw.get("modal_price") or raw.get("max_price"),
            "price_type": "wholesale",
            "spread": self._spread(raw),
            "metadata": raw.get("metadata", {}),
            "source": f"{self.organization.lower()}_scraper",
            "fetched_at": datetime.datetime.now().isoformat(),
            "ingested_at": datetime.datetime.now().isoformat(),
            "status": "ready",
        }

    def _spread(self, r: Dict) -> Optional[float]:
        if r.get("min_price") and r.get("max_price"):
            return r["max_price"] - r["min_price"]
        return None

    def run(self) -> Dict:
        stats = {
            "organization": self.organization,
            "state": self.state_code,
            "start_time": datetime.datetime.now(),
            "commodities_fetched": 0,
            "records_scraped": 0,
            "records_upserted": 0,
            "errors": 0,
            "success": False,
        }

        try:
            logger.info("Fetching commodities...")
            commodities = self.fetch_commodities()
            stats["commodities_fetched"] = len(commodities)

            all_records = []

            for code, name in commodities.items():
                try:
                    records = self.fetch_commodity_prices(code, name)
                    if records:
                        all_records.extend(self.normalize_record(r) for r in records)
                    time.sleep(self.config.get("rate_limit_delay", 1.5))
                except Exception as e:
                    logger.error(f"Commodity error {code}: {e}")
                    stats["errors"] += 1

            stats["records_scraped"] = len(all_records)

            if all_records:
                self._upsert(all_records)
                stats["records_upserted"] = len(all_records)
                stats["success"] = True

        except Exception as e:
            stats["error_message"] = str(e)
            logger.exception("Fatal scraper error")

        stats["end_time"] = datetime.datetime.now()
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        self._update_source(stats)
        self._log_summary(stats)
        return stats

    def _upsert(self, rows: List[Dict], batch: int = 100):
        for i in range(0, len(rows), batch):
            self.supabase.table("market_prices").upsert(
                rows[i : i + batch],
                on_conflict="source_id,commodity_code,price_date,market_location",
            ).execute()

    def _update_source(self, stats: Dict):
        try:
            self.supabase.table("agri_market_sources").update(
                {
                    "last_checked_at": datetime.datetime.now().isoformat(),
                    "metadata": {"last_run": stats, "last_success": stats["success"]},
                }
            ).eq("id", self.source_id).execute()
        except Exception:
            pass

    def _log_summary(self, s: Dict):
        logger.info(
            f"{self.organization} | success={s['success']} | records={s['records_upserted']} | errors={s['errors']}"
        )


# ============================================================
# MSAMB SCRAPER (matches local script)
# ============================================================
class MSAMBScraper(BaseAPMCScraper):
    def __init__(self, supabase: Client, config: Dict):
        super().__init__(supabase, config)
        import requests
        from bs4 import BeautifulSoup

        self.session = requests.Session()
        self.BeautifulSoup = BeautifulSoup
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )

        if config.get("page_requires_session"):
            self._establish_session()

    def _establish_session(self):
        url = build_url(self.config["base_url"], self.config["main_page"])
        r = self.session.get(url, timeout=20)
        r.raise_for_status()
        logger.info("✅ MSAMB session established")

    def fetch_commodities(self) -> Dict[str, str]:
        path = self.config.get("commodity_html_path")
        if not os.path.isabs(path):
            path = os.path.join(os.getenv("COMMODITY_HTML_DIR", "."), path)

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            soup = self.BeautifulSoup(f.read(), "lxml")

        commodities = {}
        for opt in soup.select("select#drpCommodities option"):
            code = opt.get("value", "").strip()
            if code.isdigit():
                commodities[code] = opt.text.strip()
        return commodities

    def fetch_commodity_prices(self, code: str, name: str) -> List[Dict]:
        url = build_url(self.config["base_url"], self.config["data_endpoint"])
        r = self.session.get(
            url,
            params={"commodityCode": code, "apmcCode": "null"},
            timeout=30,
        )
        if r.status_code != 200:
            return []
        return self._parse(r.text, code, name)

    def _parse(self, html: str, code: str, name: str) -> List[Dict]:
        soup = self.BeautifulSoup(html, "html.parser")
        rows, current_date = [], None

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) == 1 and tds[0].get_text(strip=True):
                current_date = self.parse_date(tds[0].text)
                continue

            if len(tds) >= 7 and current_date:
                rows.append(
                    {
                        "commodity_code": code,
                        "crop_name": name,
                        "market_location": tds[0].text.strip(),
                        "variety": tds[1].text.strip(),
                        "unit": tds[2].text.strip(),
                        "arrival": self._num(tds[3].text),
                        "min_price": self._num(tds[4].text),
                        "max_price": self._num(tds[5].text),
                        "modal_price": self._num(tds[6].text),
                        "price_date": current_date.isoformat(),
                        "metadata": {"source": "msamb"},
                    }
                )
        return rows

    def parse_date(self, s: str) -> datetime.date:
        try:
            return datetime.datetime.strptime(s.strip(), "%d/%m/%Y").date()
        except Exception:
            return datetime.date.today()

    def _num(self, v: str) -> Optional[float]:
        import re

        v = re.sub(r"[^0-9.]", "", v)
        return float(v) if v else None


# ============================================================
# SCRAPER FACTORY
# ============================================================
class ScraperFactory:
    SCRAPERS = {"MSAMB": MSAMBScraper}

    @classmethod
    def create(cls, supabase: Client, config: Dict) -> BaseAPMCScraper:
        scraper_cls = cls.SCRAPERS.get(config.get("organization"))
        if not scraper_cls:
            raise ValueError("No scraper registered")
        return scraper_cls(supabase, config)


# ============================================================
# ORCHESTRATOR
# ============================================================
class APMCOrchestrator:
    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    def run_all(self):
        # ❗ Fetch ALL rows (no boolean filter)
        resp = self.supabase.table("agri_market_sources").select("*").execute()
        rows = resp.data or []

        # ✅ Client-side boolean filter (SAFE)
        sources = [r for r in rows if r.get("active") is True]

        logger.info("Loaded %d active agri_market_sources", len(sources))

        results = []
        for src in sources:
            stats = {"organization": src.get("organization"), "success": False}
            try:
                scraper = ScraperFactory.create(self.supabase, src)
                stats.update(scraper.run())
            except Exception as e:
                stats["error"] = str(e)
                logger.exception("Scraper failed for source %s", src.get("organization"))
            results.append(stats)

        # Exit non-zero only if ALL scrapers failed
        if results and all(not r.get("success") for r in results):
            sys.exit(1)


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    APMCOrchestrator().run_all()
