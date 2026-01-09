import streamlit as st
import pandas as pd
import requests
from pathlib import Path

DOWNLOAD_URL = "https://drive.google.com/uc?export=download"
LOCAL_PATH = Path("jobbank_master.parquet")

# Read from Streamlit Secrets
GDRIVE_FILE_ID = st.secrets["GDRIVE_FILE_ID"]


def is_parquet_file(path: Path) -> bool:
    """Parquet files usually start with b'PAR1'."""
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"


def download_from_gdrive(file_id: str, destination: Path):
    session = requests.Session()

    r = session.get(DOWNLOAD_URL, params={"id": file_id}, stream=True, timeout=180)
    r.raise_for_status()

    # Try to find confirmation token (large file warning)
    token = None
    for k, v in r.cookies.items():
        if k.startswith("download_warning"):
            token = v
            break

    if token:
        r = session.get(
            DOWNLOAD_URL,
            params={"id": file_id, "confirm": token},
            stream=True,
            timeout=180,
        )
        r.raise_for_status()

    # Save file
    with open(destination, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


@st.cache_data(show_spinner=True)
def load_data(force_redownload: bool = False):
    if force_redownload and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if (not LOCAL_PATH.exists()) or (not is_parquet_file(LOCAL_PATH)):
        st.info("Downloading dataset from Google Drive...")
        # Ensure no bad file remains
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()

        download_from_gdrive(GDRIVE_FILE_ID, LOCAL_PATH)

        # Validate download
        if not is_parquet_file(LOCAL_PATH):
            # Show a hint for debugging (size)
            size = LOCAL_PATH.stat().st_size if LOCAL_PATH.exists() else 0
            raise RuntimeError(
                f"Downloaded file is not a valid Parquet (magic bytes not found). "
                f"Downloaded size={size} bytes. "
                f"Google Drive likely returned an HTML warning/redirect page."
            )

    return pd.read_parquet(LOCAL_PATH)


st.set_page_config(page_title="JobBank Dashboard", layout="wide")
st.title("Canada Job Bank Dashboard")

# Manual refresh button (clears cache + re-download)
if st.button("🔄 Refresh data from Google Drive"):
    st.cache_data.clear()
    df = load_data(force_redownload=True)
else:
    df = load_data()

st.success(f"Loaded {len(df):,} rows")
st.dataframe(df.head(100), use_container_width=True)
