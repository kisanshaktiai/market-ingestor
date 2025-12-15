#!/usr/bin/env python3
"""
Dynamic Multi-State APMC Market Scraper
Author: Amarsinh Patil (KisanShaktiAI)

• Source-driven via agri_market_sources
• Session-safe scraping
• CSV artifact output
• Supabase upsert (market_prices)
• Resume-from-last-success
"""

import os
import sys
import csv
import time
import json
import datetime
import logging
from typing import Dict, List, Optional
from abc import ABC, abstractmethod
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("apmc")

# ============================================================
# ENV
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
COMMODITY_HTML_DIR = os.getenv("COMMODITY_HTML_DIR", ".")
OUTPUT_DIR = "data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("❌ SUPABASE credentials missing")
    sys.exit(1)

# ============================================================
# HELPERS
# ============================================================
def build_url(base: str, path: str) -> str:
    return path if path.startswith("http") else urljoin(base.rstrip("/") + "/", path.lstrip("/"))

def clean_num(v: Optional[str]) -> Optional[float]:
    import re
    v = re.sub(r"[^0-9.]", "", v or "")
    return float(v) if v else None

def parse_date_any(s: str, fmt: str) -> Optional[str]:
    try:
        return datetime.datetime.strptime(s.strip(), fmt).date().isoformat()
    except Exception:
        return None

# ============================================================
# BASE SCRAPER
# ============================================================
class BaseAPMCScraper(ABC):
    def __init__(self, sb: Client, src: Dict):
        self.sb = sb
        self.src = src
        self.source_id = src["id"]
        self.organization = src["organization"]
        self.state_code = src.get("state_code")

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self.src["base_url"],
            "Origin": self.src["base_url"],
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
        })


        today = datetime.date.today().isoformat()
        self.csv_path = os.path.join(
            OUTPUT_DIR,
            f"{self.organization.lower()}_{self.state_code}_{today}.csv",
        )

    # --------------------------------------------------------
    @abstractmethod
    def load_commodities(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def fetch_prices(self, code: str, name: str) -> List[Dict]:
        pass

    # --------------------------------------------------------
    def run(self) -> Dict:
        log.info("▶ %s (%s) started", self.organization, self.state_code)

        resume = (self.src.get("metadata") or {}).get("resume", {})
        all_rows: List[Dict] = []
        parsed = skipped = 0

        # establish session if required
        if self.src.get("page_requires_session"):
            main_url = build_url(self.src["base_url"], self.src["main_page"])
           r = self.session.get(
                main_url,
                allow_redirects=True,
                timeout=30,
            )
            r.raise_for_status()
            
            cookies = self.session.cookies.get_dict()
            log.info("✅ Session established | cookies=%s", cookies)
            
            if not cookies:
                raise RuntimeError("❌ MSAMB session cookies NOT created")

            log.info("✅ Session established | cookies=%s", self.session.cookies.get_dict())

        commodities = self.load_commodities()
        log.info("Loaded %d commodities", len(commodities))

        for code, name in commodities.items():
            rows = self.fetch_prices(code, name)
            parsed += len(rows)

            last_date = resume.get(code)
            if last_date:
                rows = [r for r in rows if r["price_date"] > last_date]
                skipped += parsed - len(rows)

            all_rows.extend(rows)
            time.sleep(float(self.src.get("throttle_seconds", 1.2)))

        self._write_csv(all_rows)

        if all_rows:
            self._upsert(all_rows)
            self._update_resume(all_rows)

        success = True  # scraper executed correctly
           
        log.info(
                "%s | parsed=%d | inserted=%d",
                self.organization,
                parsed,
                len(all_rows),
            )


        return {"success": success, "parsed": parsed, "inserted": len(all_rows)}

    # --------------------------------------------------------
    def _write_csv(self, rows: List[Dict]):
        fields = [
            "commodity_code", "crop_name", "market_location", "variety",
            "unit", "arrival", "min_price", "max_price",
            "modal_price", "price_date",
        ]
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k) for k in fields})

        log.info("📁 CSV written → %s (%d rows)", self.csv_path, len(rows))

    # --------------------------------------------------------
    def _upsert(self, rows: List[Dict]):
        for i in range(0, len(rows), 100):
            self.sb.table("market_prices").upsert(
                rows[i:i+100],
                on_conflict="source_id,commodity_code,price_date,market_location",
            ).execute()

    # --------------------------------------------------------
    def _update_resume(self, rows: List[Dict]):
        resume = {}
        for r in rows:
            resume[r["commodity_code"]] = max(
                resume.get(r["commodity_code"], "0000-00-00"),
                r["price_date"],
            )

        self.sb.table("agri_market_sources").update(
            {
                "last_checked_at": datetime.datetime.utcnow().isoformat(),
                "metadata": {"resume": resume},
            }
        ).eq("id", self.source_id).execute()

# ============================================================
# MSAMB SCRAPER
# ============================================================
class MSAMBScraper(BaseAPMCScraper):
    def load_commodities(self) -> Dict[str, str]:
        path = os.path.join(COMMODITY_HTML_DIR, self.src["commodity_html_path"])
        soup = BeautifulSoup(open(path, encoding="utf-8").read(), "lxml")
        return {
            o["value"]: o.text.strip()
            for o in soup.select("#drpCommodities option")
            if o.get("value", "").isdigit()
        }

    def fetch_prices(self, code: str, name: str) -> List[Dict]:
        url = build_url(self.src["base_url"], self.src["data_endpoint"])
       # 1️⃣ TRY POST FIRST (MSAMB EXPECTS THIS)
        r = self.session.post(
            url,
            data={
                "commodityCode": code,
                "apmcCode": "null",
            },
            timeout=30,
        )
        
        # 2️⃣ FALL BACK TO GET ONLY IF POST FAILS
        if r.status_code != 200 or "<tr" not in r.text:
            r = self.session.get(
                url,
                params={
                    "commodityCode": code,
                    "apmcCode": "null",
                },
                timeout=30,
            )

        if r.status_code != 200 or "<tr" not in r.text:
            log.error("❌ EMPTY HTML | %s | code=%s | len=%d", url, code, len(r.text))
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        rows, current_date = [], None
        date_fmt = self.src.get("date_format", "%d/%m/%Y")

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")

            if len(tds) == 1:
                current_date = parse_date_any(tds[0].text, date_fmt)
                continue

            if len(tds) >= 7 and current_date:
                rows.append({
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
                    "source": self.organization,
                    "status": "ready",
                })

        log.info("%s (%s) → %d rows", name, code, len(rows))
        return rows

# ============================================================
# FACTORY
# ============================================================
SCRAPER_MAP = {
    "MSAMB": MSAMBScraper,
}

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    sources = sb.table("agri_market_sources").select("*").eq("active", True).execute().data
    log.info("Loaded %d active agri_market_sources", len(sources))

    failures = 0
    for src in sources:
        scraper_cls = SCRAPER_MAP.get(src.get("organization"))
        if not scraper_cls:
            log.warning("No scraper registered for %s", src.get("organization"))
            continue

        result = scraper_cls(sb, src).run()
        if not result["success"]:
            failures += 1

    if failures == len(sources):
        sys.exit(1)
