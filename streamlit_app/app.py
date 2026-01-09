import re
import streamlit as st
import pandas as pd
import requests
from pathlib import Path

LOCAL_PATH = Path("jobbank_master.parquet")
GDRIVE_FILE_ID = st.secrets["GDRIVE_FILE_ID"]

BASE_URL = "https://drive.google.com/uc?export=download"


def is_parquet(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"


def _save_stream(resp: requests.Response, dest: Path) -> None:
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


def download_from_gdrive(file_id: str, dest: Path) -> None:
    """
    Robust Google Drive downloader:
    - Works with virus scan warning HTML for large files
    - Extracts confirm token from cookie OR from the warning HTML download link
    """
    session = requests.Session()

    # First request (often returns HTML warning page for big files)
    r = session.get(BASE_URL, params={"id": file_id}, stream=True, timeout=300)
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()

    # If Google already returns the file, save directly
    if "text/html" not in ctype:
        _save_stream(r, dest)
        return

    # 1) Try cookie token
    token = None
    for k, v in r.cookies.items():
        if k.startswith("download_warning"):
            token = v
            break

    # 2) Parse token from HTML (virus scan page)
    if token is None:
        html = r.text

        # Look for /uc?export=download&confirm=XXXX&id=FILE_ID (may be HTML-escaped)
        patterns = [
            rf'href="\/uc\?export=download&amp;confirm=([0-9A-Za-z_]+)&amp;id={re.escape(file_id)}',
            rf'href="\/uc\?export=download&confirm=([0-9A-Za-z_]+)&id={re.escape(file_id)}',
            rf'confirm=([0-9A-Za-z_]+)&amp;id={re.escape(file_id)}',
            rf'confirm=([0-9A-Za-z_]+)&id={re.escape(file_id)}',
        ]
        for p in patterns:
            m = re.search(p, html)
            if m:
                token = m.group(1)
                break

    if token is None:
        html_sample = r.text[:700]
        raise RuntimeError(
            "Google Drive returned an HTML virus-scan page but confirm token was not found. "
            "Try re-sharing the file as 'Anyone with the link' OR re-upload the file to get a fresh link. "
            f"HTML sample: {html_sample}"
        )

    # Confirm download
    r2 = session.get(
        BASE_URL,
        params={"id": file_id, "confirm": token},
        stream=True,
        timeout=300,
    )
    r2.raise_for_status()
    _save_stream(r2, dest)

if not is_parquet(dest):
        # likely saved HTML instead of parquet
    with open(dest, "rb") as f:
            head = f.read(200)
    raise RuntimeError(f"Still not parquet after confirm. First bytes: {head!r}")
        
@st.cache_data(show_spinner=True)
def load_data(force: bool = False) -> pd.DataFrame:
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if not LOCAL_PATH.exists() or not is_parquet(LOCAL_PATH):
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()

        st.info("Downloading dataset from Google Drive...")
        download_from_gdrive(GDRIVE_FILE_ID, LOCAL_PATH)

        if not is_parquet(LOCAL_PATH):
            raise RuntimeError("Downloaded file is not a valid Parquet (PAR1 missing).")

    return pd.read_parquet(LOCAL_PATH)


st.set_page_config(page_title="Canada Job Bank Dashboard", layout="wide")
st.title("Canada Job Bank – Job Postings Dashboard")

if st.button("🔄 Refresh data"):
    st.cache_data.clear()
    df = load_data(force=True)
else:
    df = load_data()

st.success(f"Loaded {len(df):,} rows")
st.dataframe(df.head(100), use_container_width=True)
