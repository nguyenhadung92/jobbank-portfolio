import streamlit as st
import pandas as pd
import requests
from pathlib import Path

# =========================
# CONFIG
# =========================
GDRIVE_FILE_ID = "14uB6vKTn2M_UBGGJAw072d4LgJ1rLsCc"
DOWNLOAD_URL = "https://drive.google.com/uc?export=download"

LOCAL_PATH = Path("jobbank_master.parquet")


# =========================
# GOOGLE DRIVE DOWNLOADER
# =========================
def download_from_gdrive(file_id: str, destination: Path):
    session = requests.Session()

    response = session.get(
        DOWNLOAD_URL,
        params={"id": file_id},
        stream=True,
        timeout=120
    )
    response.raise_for_status()

    # Check for confirmation token (virus scan warning)
    token = None
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            token = value
            break

    if token:
        response = session.get(
            DOWNLOAD_URL,
            params={"id": file_id, "confirm": token},
            stream=True,
            timeout=120
        )
        response.raise_for_status()

    # Save file
    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


@st.cache_data(show_spinner=True)
def load_data():
    if not LOCAL_PATH.exists():
        st.info("Downloading dataset from Google Drive...")
        download_from_gdrive(GDRIVE_FILE_ID, LOCAL_PATH)

    return pd.read_parquet(LOCAL_PATH)


# =========================
# STREAMLIT APP
# =========================
st.set_page_config(page_title="Canada Job Bank Dashboard", layout="wide")

st.title("Canada Job Bank – Job Postings Dashboard")

df = load_data()

st.success(f"Loaded {len(df):,} job postings")

# --- quick preview ---
st.dataframe(df.head(100), use_container_width=True)
