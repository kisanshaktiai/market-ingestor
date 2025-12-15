#!/usr/bin/env python3
"""
Universal Multi-State APMC Market Price Scraper
Author: Amarsinh Patil (KisanShaktiAI)
"""

import os
import sys
import csv
import time
import logging
import datetime
from typing import Dict, List, Optional
from abc import ABC, abstractmethod
from urllib.parse import urljoin

from supabase import create_client, Client
import requests
from bs4 import BeautifulSoup

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("apmc")

# ============================================================
# ENV
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
COMMODITY_HTML_DIR = os.getenv("COMMODITY_HTML_DIR", ".")
OUTPUT_DIR = "data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE credentials missing")
    sys.exit(1)

# ============================================================
# HELPERS
# ============================================================
def build_url(base: str, path: str) -> str:
    return path if path.startswith("http") else urljoin(base.rstrip("/") + "/", path.lstrip("/"))

def clean_num(v: str) -> Optional[float]:
    import re
    v = re.sub(r"[^0-9.]", "", v or "")
    return float(v) if v else None

# ============================================================
# BASE SCRAPER
# ============================================================
class BaseAPMCScraper(ABC):
    def __init__(self, supabase: Client, src: Dict):
        self.supabase = supabase
        self.src = src
        self.source_id = src["id"]
        self.organization = src["organization"]
        self.state = src["state_code"]

        self.csv_path = os.path.join(
            OUTPUT_DIR,
            f"{self.organization.lower()}_{self.state}_{datetime.date.today()}.csv",
        )

    @abstractmethod
    def fetch_commodities(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def fetch_prices(self, code: str, name: str) -> List[Dict]:
        pass

    def run(self) -> Dict:
        logger.info(f"▶ Starting {self.organization} ({self.state})")

        resume_map = (self.src.get("metadata") or {}).get("resume", {})
        parsed = skipped = inserted = 0
        all_rows = []

        commodities = self.fetch_commodities()
        logger.info(f"Loaded {len(commodities)} commodities")

        for code, name in commodities.items():
            rows = self.fetch_prices(code, name)
            parsed += len(rows)

            last_date = resume_map.get(code)
            if last_date:
                rows = [r for r in rows if r["price_date"] > last_date]

            skipped += parsed - len(rows)
            all_rows.extend(rows)

            time.sleep(1.2)

        logger.info(f"Parsed rows: {parsed}")
        logger.info(f"Skipped by resume: {skipped}")
        logger.info(f"Rows to upsert: {len(all_rows)}")

        self._write_csv(all_rows)

        if all_rows:
            self._upsert(all_rows)
            inserted = len(all_rows)

            new_resume = resume_map.copy()
            for r in all_rows:
                code = r["commodity_code"]
                new_resume[code] = max(
                    new_resume.get(code, "0000-00-00"), r["price_date"]
                )

            self._update_resume(new_resume)

        logger.info(f"CSV written: {self.csv_path}")
        logger.info(f"Inserted: {inserted}")

        return {
            "success": True,
            "parsed": parsed,
            "skipped": skipped,
            "inserted": inserted,
        }

    def _write_csv(self, rows: List[Dict]):
        fields = [
            "commodity_code",
            "crop_name",
            "market_location",
            "variety",
            "unit",
            "arrival",
            "min_price",
            "max_price",
            "modal_price",
            "price_date",
        ]
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k) for k in fields})

    def _upsert(self, rows: List[Dict]):
        for i in range(0, len(rows), 100):
            self.supabase.table("market_prices").upsert(
                rows[i : i + 100],
                on_conflict="source_id,commodity_code,price_date,market_location",
            ).execute()

    def _update_resume(self, resume: Dict):
        self.supabase.table("agri_market_sources").update(
            {
                "last_checked_at": datetime.datetime.utcnow().isoformat(),
                "metadata": {"resume": resume},
            }
        ).eq("id", self.source_id).execute()

# ============================================================
# MSAMB SCRAPER
# ============================================================
class MSAMBScraper(BaseAPMCScraper):
    def __init__(self, supabase: Client, src: Dict):
        super().__init__(supabase, src)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

        if src.get("page_requires_session"):
            self.session.get(build_url(src["base_url"], src["main_page"]), timeout=20)

    def fetch_commodities(self) -> Dict[str, str]:
        path = os.path.join(COMMODITY_HTML_DIR, self.src["commodity_html_path"])
        soup = BeautifulSoup(open(path, encoding="utf-8").read(), "lxml")
        return {
            o["value"]: o.text.strip()
            for o in soup.select("#drpCommodities option")
            if o.get("value", "").isdigit()
        }

    def fetch_prices(self, code: str, name: str) -> List[Dict]:
        url = build_url(self.src["base_url"], self.src["data_endpoint"])
        r = self.session.get(url, params={"commodityCode": code, "apmcCode": ""}, timeout=30)
        if r.status_code != 200:
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        rows, current_date = [], None

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) == 1:
                try:
                    current_date = datetime.datetime.strptime(
                        tds[0].text.strip(), "%d/%m/%Y"
                    ).date().isoformat()
                except Exception:
                    pass
                continue

            if len(tds) >= 7 and current_date:
                rows.append(
                    {
                        "source_id": self.source_id,
                        "commodity_code": code,
                        "crop_name": name,
                        "market_location": tds[0].text.strip(),
                        "variety": tds[1].text.strip(),
                        "unit": tds[2].text.strip(),
                        "arrival": clean_num(tds[3].text),
                        "min_price": clean_num(tds[4].text),
                        "max_price": clean_num(tds[5].text),
                        "modal_price": clean_num(tds[6].text),
                        "price_date": current_date,
                        "price_per_unit": clean_num(tds[6].text) or 0,
                        "source": "msamb",
                        "status": "ready",
                    }
                )
        logger.info(f"{name} ({code}) → fetched {len(rows)} rows")
        return rows

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    sources = sb.table("agri_market_sources").select("*").execute().data
    logger.info(f"Loaded {len(sources)} active agri_market_sources")

    for src in sources:
        if src.get("organization") == "MSAMB":
            MSAMBScraper(sb, src).run()
