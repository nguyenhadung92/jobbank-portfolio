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
    session = requests.Session()

    # Step 1: get the warning page
    r = session.get(BASE_URL, params={"id": file_id}, timeout=300)
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()

    # If it's already a file, stream-download directly
    if "text/html" not in ctype:
        r_stream = session.get(BASE_URL, params={"id": file_id}, stream=True, timeout=300)
        _save_stream(r_stream, dest)
        return

    html = r.text

    # Step 2: Extract the REAL "Download anyway" link from HTML
    # It usually looks like: /uc?export=download&confirm=...&id=...&uuid=...
    m = re.search(r'href="(\/uc\?export=download[^"]+)"', html)
    if not m:
        # sometimes it appears without quotes style changes
        m = re.search(r'(\/uc\?export=download[^"&]+[^"]*)', html)

    if not m:
        raise RuntimeError("Could not find the /uc?export=download... link in the virus warning page HTML.")

    download_path = m.group(1)

    # HTML uses &amp; - convert to normal &
    download_path = download_path.replace("&amp;", "&")

    download_url = "https://drive.google.com" + download_path

    # Step 3: download using the extracted URL (this includes uuid/extra params)
    r2 = session.get(download_url, stream=True, timeout=300)
    r2.raise_for_status()
    _save_stream(r2, dest)

    # Step 4: validate parquet
    if not is_parquet(dest):
        with open(dest, "rb") as f:
            head = f.read(300)
        raise RuntimeError(f"Still not parquet. First bytes: {head!r}")



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
