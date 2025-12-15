#!/usr/bin/env python3
"""
Dynamic Multi-State APMC Market Scraper
Author: Amarsinh Patil (KisanShaktiAI)

• Source-driven via agri_market_sources
• Works with MSAMB and future APMCs
• Session-safe (header based, cookie optional)
• Robust HTML parsing
• Regex-based date detection (FIXED ROOT CAUSE)
• CSV artifact output
• Supabase upsert (market_prices)
• Resume-from-last-success
"""

import os
import sys
import csv
import time
import re
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
    v = re.sub(r"[^0-9.]", "", v or "")
    return float(v) if v else None

def parse_date_flexible(text: str) -> Optional[str]:
    """
    Robust date parser:
    Handles 15/12/2025, 15-12-2025, 15.12.2025
    """
    if not text:
        return None

    m = re.search(r"(\d{1,2})\D(\d{1,2})\D(\d{4})", text)
    if not m:
        return None

    d, mth, y = m.groups()
    try:
        return datetime.date(int(y), int(mth), int(d)).isoformat()
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
            "Referer": src.get("base_url"),
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

        # Establish session if required
        if self.src.get("page_requires_session"):
            main_url = build_url(self.src["base_url"], self.src["main_page"])
            r = self.session.get(main_url, allow_redirects=True, timeout=30)
            r.raise_for_status()
            log.info("✅ Session initialized (cookies=%s)", self.session.cookies.get_dict())

            # Warm-up AJAX call (critical for MSAMB)
            try:
                self.session.post(
                    build_url(self.src["base_url"], self.src["data_endpoint"]),
                    data={"commodityCode": "08035", "apmcCode": "null"},
                    timeout=20,
                )
            except Exception:
                pass

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

        log.info(
            "%s | parsed=%d | inserted=%d",
            self.organization,
            parsed,
            len(all_rows),
        )

        return {"success": True, "parsed": parsed, "inserted": len(all_rows)}

    # --------------------------------------------------------
    def _write_csv(self, rows: List[Dict]):
        fields = [
            "commodity_code", "crop_name", "market_location", "variety",
            "unit", "arrival", "min_price", "max_price",
            "modal_price", "price_date",
        ]
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: r.get(k) for k in fields})

        log.info("📁 CSV written → %s (%d rows)", self.csv_path, len(rows))

    # --------------------------------------------------------
    
    def _upsert(self, rows: List[Dict]):
    
    deduped = {}

    for r in rows:
        key = (
            r["source_id"],
            r["commodity_code"],
            r["price_date"],
            r["market_location"],
        )

        # keep the row with higher modal price (or last one)
        if key not in deduped:
            deduped[key] = r
        else:
            old = deduped[key]
            if (r.get("modal_price") or 0) > (old.get("modal_price") or 0):
                deduped[key] = r

        final_rows = list(deduped.values())
    
        log.info(
            "🔁 Deduplicated rows: %d → %d",
            len(rows),
            len(final_rows),
        )
    
        for i in range(0, len(final_rows), 100):
            self.sb.table("market_prices").upsert(
                final_rows[i:i + 100],
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
        with open(path, encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "lxml")

        return {
            o["value"]: o.text.strip()
            for o in soup.select("#drpCommodities option")
            if o.get("value", "").isdigit()
        }

    def fetch_prices(self, code: str, name: str) -> List[Dict]:
        url = build_url(self.src["base_url"], self.src["data_endpoint"])

        # POST (primary)
        r = self.session.post(
            url,
            data={"commodityCode": code, "apmcCode": "null"},
            timeout=30,
        )

        # GET fallback
        if r.status_code != 200 or "<tr" not in r.text:
            r = self.session.get(
                url,
                params={"commodityCode": code, "apmcCode": "null"},
                timeout=30,
            )

        if r.status_code != 200 or "<tr" not in r.text:
            log.warning("⚠️ No data | %s (%s)", name, code)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        rows: List[Dict] = []
        current_date: Optional[str] = None

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")

            # Date row
            if len(tds) == 1 or (len(tds) >= 1 and tds[0].has_attr("colspan")):
                current_date = parse_date_flexible(tds[0].get_text(strip=True))
                continue

            # Data row
            if len(tds) >= 7 and current_date:
                rows.append({
                    "source_id": self.source_id,
                    "commodity_code": code,
                    "crop_name": name,
                    "market_location": tds[0].get_text(strip=True),
                    "variety": tds[1].get_text(strip=True),
                    "unit": tds[2].get_text(strip=True),
                    "arrival": clean_num(tds[3].get_text()),
                    "min_price": clean_num(tds[4].get_text()),
                    "max_price": clean_num(tds[5].get_text()),
                    "modal_price": clean_num(tds[6].get_text()),
                    "price_date": current_date,
                    "price_per_unit": clean_num(tds[6].get_text()) or 0,
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

    sources = (
        sb.table("agri_market_sources")
        .select("*")
        .eq("active", True)
        .execute()
        .data
    )

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
