from __future__ import annotations

import re
import csv
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, List

import requests
import pandas as pd


# =========================
# CONFIG
# =========================
DATASET_ID = "ea639e28-c0fc-48bf-b5dd-b8899bd43072"
BASE_URL = "https://open.canada.ca/data/api/3/action/package_show"

START_YEAR = 2024
CURRENT_YEAR = datetime.now().year

# Output folders/files
RAW_MONTHLY_DIR = Path("data/raw_monthly")         # save monthly raw (optional but recommended)
RAW_MONTHLY_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

MASTER_PARQUET = PROCESSED_DIR / "jobbank_master.parquet"
MASTER_CSV = PROCESSED_DIR / "jobbank_master.csv"

STATE_DIR = Path("data/state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "downloaded_months.txt"

# Choose whether to also save each month raw CSV
SAVE_MONTHLY_RAW_CSV = True

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "jobbank-raw-etl/1.0"})


MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}


# =========================
# HELPERS
# =========================
def english_only(name_lower: str) -> bool:
    """Filter out resources that look explicitly French."""
    return not any(x in name_lower for x in ["french", "français", "francais"])


def extract_year_month(name: str) -> Optional[Tuple[int, int]]:
    """Extract (year, month) from resource name using month word + 4-digit year."""
    s = name.lower()

    year_m = re.search(r"\b(20\d{2})\b", s)
    if not year_m:
        return None
    year = int(year_m.group(1))

    month_name = None
    for m in MONTHS:
        if re.search(rf"\b{m}\b", s):
            month_name = m
            break
    if not month_name:
        return None

    return year, MONTHS[month_name]


def ym_key(year: int, month: int) -> str:
    return f"{year}-{month:02d}"


def load_state() -> set[str]:
    if not STATE_PATH.exists():
        return set()
    return set(
        x.strip() for x in STATE_PATH.read_text(encoding="utf-8").splitlines()
        if x.strip()
    )


def save_state(state: set[str]) -> None:
    STATE_PATH.write_text("\n".join(sorted(state)) + "\n", encoding="utf-8")


def sniff_delimiter(sample_text: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ","


def read_csv_robust_from_bytes(content: bytes) -> Tuple[pd.DataFrame, str, str]:
    """
    Read CSV robustly without changing column names/meaning:
      - auto detect delimiter
      - try encodings
      - tolerant parsing to avoid ParserError
    Returns df + encoding + delimiter.
    """
    sample = content[:20000].decode("latin1", errors="replace")
    sep = sniff_delimiter(sample)

    encodings_to_try = ["utf-8", "utf-8-sig", "latin1", "utf-16"]
    last_err = None

    for enc in encodings_to_try:
        try:
            bio = BytesIO(content)
            df = pd.read_csv(
                bio,
                encoding=enc,
                sep=sep,
                engine="python",
                on_bad_lines="skip",
            )

            # If delimiter sniff failed and got 1 column, try TSV
            if df.shape[1] == 1 and sep != "\t":
                bio2 = BytesIO(content)
                df2 = pd.read_csv(
                    bio2,
                    encoding=enc,
                    sep="\t",
                    engine="python",
                    on_bad_lines="skip",
                )
                if df2.shape[1] > 1:
                    return df2, enc, "\t"

            return df, enc, sep
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Could not parse CSV bytes. Last error: {last_err}")


def list_monthly_resources_dedup() -> List[Dict]:
    """
    List monthly resources, deduplicated so each YYYY-MM is chosen once.
    If multiple resources exist for same month, keep the one with latest 'created'.
    """
    resp = SESSION.get(BASE_URL, params={"id": DATASET_ID}, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        raise RuntimeError("CKAN API failed")

    resources = data["result"]["resources"]

    by_month: Dict[str, Dict] = {}

    for r in resources:
        if str(r.get("format", "")).upper() != "CSV":
            continue

        name = r.get("name", "")
        name_l = name.lower()

        if not english_only(name_l):
            continue

        ym_tuple = extract_year_month(name)
        if not ym_tuple:
            continue

        year, month = ym_tuple
        if year < START_YEAR or year > CURRENT_YEAR:
            continue

        ym = ym_key(year, month)

        candidate = {
            "year": year,
            "month": month,
            "ym": ym,
            "name": name,
            "url": r.get("url", ""),
            "created": r.get("created", ""),
        }

        if ym not in by_month:
            by_month[ym] = candidate
        else:
            # keep the latest created
            if candidate["created"] > by_month[ym]["created"]:
                by_month[ym] = candidate

    out = list(by_month.values())
    out.sort(key=lambda x: (x["year"], x["month"]))
    return out


def load_master() -> Optional[pd.DataFrame]:
    if MASTER_PARQUET.exists():
        return pd.read_parquet(MASTER_PARQUET)
    return None


def save_master(df: pd.DataFrame) -> None:
    df.to_parquet(MASTER_PARQUET, index=False)
    df.to_csv(MASTER_CSV, index=False)


# =========================
# MAIN
# =========================
def main() -> None:
    state = load_state()
    resources = list_monthly_resources_dedup()

    print(f"Found {len(resources)} monthly CSV resources (EN, deduped).")
    new_months = [r for r in resources if r["ym"] not in state]

    if not new_months:
        print("No new months found. Master is up to date.")
        return

    print("New months to fetch:", [r["ym"] for r in new_months])

    master = load_master()
    new_frames = []

    for r in new_months:
        ym = r["ym"]
        year = r["year"]
        month = r["month"]

        print(f"\n⬇️ Fetching {ym} | {r['name']}")

        content = SESSION.get(r["url"], timeout=120).content

        # Parse raw (no semantic mapping)
        df, enc_used, sep_used = read_csv_robust_from_bytes(content)

        # Add only minimal columns for tracking/time (does not change meaning)
        df["year"] = year
        df["month"] = month
        df["year_month"] = ym
        df["month_start"] = pd.to_datetime(f"{ym}-01")
        df["source_resource_name"] = r["name"]
        df["source_url"] = r["url"]
        df["encoding_used"] = enc_used
        df["delimiter_used"] = sep_used

        # Save monthly raw CSV (optional)
        if SAVE_MONTHLY_RAW_CSV:
            out_dir = RAW_MONTHLY_DIR / str(year)
            out_dir.mkdir(parents=True, exist_ok=True)
            monthly_path = out_dir / f"{ym}.csv"
            df.to_csv(monthly_path, index=False)

        new_frames.append(df)
        state.add(ym)

        print(f"✅ Parsed {ym} | rows={len(df):,} cols={df.shape[1]} (enc={enc_used}, sep={repr(sep_used)})")

    # Update master
    new_all = pd.concat(new_frames, ignore_index=True, sort=False)

    if master is None:
        combined = new_all
    else:
        combined = pd.concat([master, new_all], ignore_index=True, sort=False)

    # NOTE: We don't enforce semantic dedup because raw dataset may not have stable job_id.
    # Keep it simple: drop exact duplicate rows only.
    combined = combined.drop_duplicates()

    save_master(combined)
    save_state(state)

    print("\n🎉 ETL done.")
    print(f"Master parquet: {MASTER_PARQUET}")
    print(f"Master csv:     {MASTER_CSV}")
    print(f"Total rows:     {len(combined):,}")
    print(f"Total cols:     {combined.shape[1]}")


if __name__ == "__main__":
    main()
