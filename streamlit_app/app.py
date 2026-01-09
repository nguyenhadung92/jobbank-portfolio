import streamlit as st
import pandas as pd
import requests
from pathlib import Path

st.set_page_config(page_title="Canada Job Bank Dashboard", layout="wide")

DATA_URL = st.secrets["DATA_URL"]
LOCAL_PATH = Path("jobbank_master.parquet")

def is_parquet(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 4 and p.read_bytes()[:4] == b"PAR1"

def download(url: str, dest: Path) -> None:
    r = requests.get(url, stream=True, allow_redirects=True, timeout=300)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

@st.cache_data(show_spinner=True)
def load_data(force: bool = False) -> pd.DataFrame:
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if (not LOCAL_PATH.exists()) or (not is_parquet(LOCAL_PATH)):
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()
        download(DATA_URL, LOCAL_PATH)

    return pd.read_parquet(LOCAL_PATH)

# ================= UI =================
st.title("🇨🇦 Canada Job Bank – Job Postings Dashboard")

if st.button("🔄 Refresh data"):
    st.cache_data.clear()
    df = load_data(force=True)
else:
    df = load_data()

st.success(f"Loaded {len(df):,} rows")
st.dataframe(df.head(100), use_container_width=True)
