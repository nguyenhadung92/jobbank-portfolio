import streamlit as st
import pandas as pd
import requests
from pathlib import Path

DOWNLOAD_URL = "https://drive.google.com/uc?export=download"
LOCAL_PATH = Path("jobbank_master.parquet")

# ✅ read from Streamlit Secrets
GDRIVE_FILE_ID = st.secrets["GDRIVE_FILE_ID"]


def download_from_gdrive(file_id: str, destination: Path):
    session = requests.Session()

    r = session.get(DOWNLOAD_URL, params={"id": file_id}, stream=True, timeout=120)
    r.raise_for_status()

    # handle virus-scan warning token (large files)
    token = None
    for k, v in r.cookies.items():
        if k.startswith("download_warning"):
            token = v
            break

    if token:
        r = session.get(DOWNLOAD_URL, params={"id": file_id, "confirm": token}, stream=True, timeout=120)
        r.raise_for_status()

    with open(destination, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


@st.cache_data(show_spinner=True)
def load_data():
    if not LOCAL_PATH.exists():
        download_from_gdrive(GDRIVE_FILE_ID, LOCAL_PATH)
    return pd.read_parquet(LOCAL_PATH)


st.set_page_config(page_title="JobBank Dashboard", layout="wide")
st.title("Canada Job Bank Dashboard")

df = load_data()
st.success(f"Loaded {len(df):,} rows")
st.dataframe(df.head(100), use_container_width=True)
