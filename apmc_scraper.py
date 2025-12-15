#!/usr/bin/env python3
"""
Universal Multi-State APMC Market Price Scraper
Author: Amarsinh Patil (KisanShaktiAI)
"""

import os
import sys
import time
import logging
import datetime
from typing import Dict, List, Optional
from abc import ABC, abstractmethod
from urllib.parse import urljoin

from supabase import create_client, Client

# ============================================================
# HELPERS
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
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================
# SUPABASE CONFIG
# ============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)


# ============================================================
# BASE SCRAPER
# ============================================================

class BaseAPMCScraper(ABC):
    def __init__(self, supabase: Client, config: Dict):
        self.supabase = supabase
        self.config = config
        self.source_id = config["id"]
        self.organization = config.get("organization")
        self.state_code = config.get("state_code")

        logger.info("Initialized %s scraper for %s", self.organization, self.state_code)

    # ---------------- Resume helpers ----------------

    def _get_resume_map(self) -> Dict[str, str]:
        meta = self.config.get("metadata") or {}
        return meta.get("resume", {})

    def _update_resume_point(self, commodity_code: str, last_date: str):
        meta = self.config.get("metadata") or {}
        resume = meta.get("resume", {})
        resume[commodity_code] = last_date
        meta["resume"] = resume

        self.supabase.table("agri_market_sources").update(
            {"metadata": meta}
        ).eq("id", self.source_id).execute()

    # ---------------- Abstract methods ----------------

    @abstractmethod
    def fetch_commodities(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def fetch_commodity_prices(self, commodity_code: str, commodity_name: str) -> List[Dict]:
        pass

    # ---------------- Normalization ----------------

    def normalize_record(self, r: Dict) -> Dict:
        return {
            "source_id": self.source_id,
            "commodity_code": r["commodity_code"],
            "crop_name": r["crop_name"],
            "variety": r.get("variety"),
            "unit": r.get("unit", "quintal"),
            "market_location": r["market_location"],
            "district": r.get("district"),
            "state": self.state_code,
            "price_date": r["price_date"],
            "arrival": r.get("arrival"),
            "min_price": r.get("min_price"),
            "max_price": r.get("max_price"),
            "modal_price": r.get("modal_price"),
            "price_per_unit": r.get("modal_price") or r.get("max_price") or 0,
            "spread": (
                r["max_price"] - r["min_price"]
                if r.get("min_price") and r.get("max_price")
                else None
            ),
            "price_type": "wholesale",
            "metadata": r.get("metadata", {}),
            "source": f"{self.organization.lower()}_scraper",
            "fetched_at": datetime.datetime.utcnow().isoformat(),
            "status": "ready",
        }

    # ---------------- Main runner ----------------

    def run(self) -> Dict:
        stats = {
            "organization": self.organization,
            "records_scraped": 0,
            "records_upserted": 0,
            "errors": 0,
            "success": False,
        }

        resume_map = self._get_resume_map()

        try:
            commodities = self.fetch_commodities()
            all_rows = []

            for code, name in commodities.items():
                try:
                    logger.info("Fetching commodity %s (%s)", code, name)

                    rows = self.fetch_commodity_prices(code, name)
                    last_date = resume_map.get(code)

                    if last_date:
                        rows = [
                            r for r in rows
                            if r.get("price_date") and r["price_date"] > last_date
                        ]

                    if rows:
                        all_rows.extend(self.normalize_record(r) for r in rows)
                        newest = max(r["price_date"] for r in rows)
                        self._update_resume_point(code, newest)

                    time.sleep(self.config.get("rate_limit_delay", 3))

                except Exception as e:
                    logger.error("Commodity error %s: %s", code, e)
                    stats["errors"] += 1

            stats["records_scraped"] = len(all_rows)

            if all_rows:
                self._upsert(all_rows)
                stats["records_upserted"] = len(all_rows)
                stats["success"] = True

        except Exception:
            logger.exception("Fatal scraper error")

        self._update_source(stats)
        logger.info(
            "%s | success=%s | records=%d | errors=%d",
            self.organization,
            stats["success"],
            stats["records_upserted"],
            stats["errors"],
        )
        return stats

    # ---------------- DB helpers ----------------

    def _upsert(self, rows: List[Dict], batch: int = 100):
        for i in range(0, len(rows), batch):
            self.supabase.table("market_prices").upsert(
                rows[i:i + batch],
                on_conflict="source_id,commodity_code,price_date,market_location",
            ).execute()

    def _update_source(self, stats: Dict):
        try:
            self.supabase.table("agri_market_sources").update(
                {
                    "last_checked_at": datetime.datetime.utcnow().isoformat(),
                    "metadata": {
                        **(self.config.get("metadata") or {}),
                        "last_run": stats,
                        "last_success": stats["success"],
                    },
                }
            ).eq("id", self.source_id).execute()
        except Exception:
            pass


# ============================================================
# MSAMB SCRAPER
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
        self.session.get(url, timeout=30)
        logger.info("MSAMB session established")

    def fetch_commodities(self) -> Dict[str, str]:
        path = self.config["commodity_html_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.getenv("COMMODITY_HTML_DIR", "."), path)

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            soup = self.BeautifulSoup(f.read(), "lxml")

        return {
            opt.get("value"): opt.text.strip()
            for opt in soup.select("select#drpCommodities option")
            if opt.get("value", "").isdigit()
        }

    def fetch_commodity_prices(self, code: str, name: str) -> List[Dict]:
        url = build_url(self.config["base_url"], self.config["data_endpoint"])

        for attempt in range(1, 4):
            try:
                r = self.session.get(
                    url,
                    params={"commodityCode": code, "apmcCode": ""},
                    timeout=45,
                )
                if r.status_code == 200 and "<tr" in r.text:
                    return self._parse(r.text, code, name)
            except Exception as e:
                logger.warning(
                    "Retry %d/3 failed for %s: %s", attempt, code, e
                )
                time.sleep(5 * attempt)

        return []

    def _parse(self, html: str, code: str, name: str) -> List[Dict]:
        soup = self.BeautifulSoup(html, "html.parser")
        rows, current_date = [], None

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) == 1:
                try:
                    current_date = datetime.datetime.strptime(
                        tds[0].text.strip(), "%d/%m/%Y"
                    ).date().isoformat()
                except Exception:
                    continue

            elif len(tds) >= 7 and current_date:
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
                        "price_date": current_date,
                        "metadata": {"source": "msamb"},
                    }
                )
        return rows

    def _num(self, v: str) -> Optional[float]:
        import re
        v = re.sub(r"[^0-9.]", "", v)
        return float(v) if v else None


# ============================================================
# FACTORY
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
        resp = self.supabase.table("agri_market_sources").select("*").execute()
        sources = [r for r in (resp.data or []) if r.get("active") is True]

        logger.info("Loaded %d active agri_market_sources", len(sources))

        results = []
        for src in sources:
            try:
                scraper = ScraperFactory.create(self.supabase, src)
                results.append(scraper.run())
            except Exception as e:
                logger.exception("Scraper failed")
                results.append({"success": False})

        # Do NOT hard-fail on temporary network issues
        if results and all(r.get("records_upserted", 0) == 0 for r in results):
            logger.warning("All scrapers returned zero records (non-fatal)")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    APMCOrchestrator().run_all()
