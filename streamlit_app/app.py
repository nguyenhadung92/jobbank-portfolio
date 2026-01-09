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
    - Handles "can't scan for viruses" warning page
    - Extracts confirm token either from cookie or from HTML
    """
    session = requests.Session()

    # 1) First request
    r = session.get(BASE_URL, params={"id": file_id}, stream=True, timeout=300)

    # If Google already returns file (not html), save it
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "text/html" not in ctype:
        _save_stream(r, dest)
        return

    # 2) Try cookie-based token
    token = None
    for k, v in r.cookies.items():
        if k.startswith("download_warning"):
            token = v
            break

    # 3) If no cookie token, parse token from HTML
    if token is None:
        html = r.text
        m = re.search(r'confirm=([0-9A-Za-z_]+)', html)
        if m:
            token = m.group(1)

    if token is None:
        # Save a small debug sample
        html_sample = r.text[:500]
        raise RuntimeError(
            "Google Drive returned an HTML page but no confirm token was found. "
            "Check sharing permission (Anyone with the link). "
            f"HTML sample: {html_sample}"
        )

    # 4) Confirm download
    r2 = session.get(
        BASE_URL,
        params={"id": file_id, "confirm": token},
        stream=True,
        timeout=300,
    )
    _save_stream(r2, dest)


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
