import streamlit as st
import pandas as pd
import requests
from pathlib import Path

DOWNLOAD_URL = "https://drive.google.com/uc?export=download"
LOCAL_PATH = Path("jobbank_master.parquet")

GDRIVE_FILE_ID = st.secrets["GDRIVE_FILE_ID"]


def is_parquet(path: Path) -> bool:
    # Parquet header bytes: b"PAR1"
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"


def download_from_gdrive(file_id: str, destination: Path) -> None:
    session = requests.Session()

    r = session.get(DOWNLOAD_URL, params={"id": file_id}, stream=True, timeout=180)
    r.raise_for_status()

    # Confirm token for large file warning
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
def load_data(force: bool = False) -> pd.DataFrame:
    # Force refresh: delete local cached file
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    # If file missing or not a real parquet -> re-download
    if (not LOCAL_PATH.exists()) or (not is_parquet(LOCAL_PATH)):
        # remove bad file if exists
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()

        st.info("Downloading parquet from Google Drive...")
        download_from_gdrive(GDRIVE_FILE_ID, LOCAL_PATH)

        # Validate it is parquet
        if not is_parquet(LOCAL_PATH):
            # show a tiny preview to debug (first bytes + size)
            size = LOCAL_PATH.stat().st_size if LOCAL_PATH.exists() else 0
            with open(LOCAL_PATH, "rb") as f:
                head = f.read(200)
            raise RuntimeError(
                "Downloaded file is NOT a valid Parquet. "
                "Google Drive likely returned HTML (permission/virus-scan page). "
                f"Size={size} bytes. First 200 bytes:\n{head!r}"
            )

    return pd.read_parquet(LOCAL_PATH)


st.set_page_config(page_title="JobBank Dashboard", layout="wide")
st.title("Canada Job Bank Dashboard")

# Button to refresh dataset (clear cache + re-download)
if st.button("🔄 Refresh data"):
    st.cache_data.clear()
    df = load_data(force=True)
else:
    df = load_data()

st.success(f"Loaded {len(df):,} rows")
st.dataframe(df.head(100), use_container_width=True)
