import streamlit as st
import pandas as pd
import requests
from pathlib import Path

LOCAL_PATH = Path("jobbank_master.parquet")

# Put this in Streamlit secrets
DATA_URL = st.secrets["DATA_URL"]

def is_parquet(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"

def download_file(url: str, dest: Path) -> None:
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

@st.cache_data(show_spinner=True)
def load_data(force=False):
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if not LOCAL_PATH.exists() or not is_parquet(LOCAL_PATH):
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()
        st.info("Downloading dataset...")
        download_file(DATA_URL, LOCAL_PATH)

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
