import streamlit as st
import pandas as pd
from pathlib import Path
import gdown

LOCAL_PATH = Path("jobbank_master.parquet")
GDRIVE_FILE_ID = st.secrets["GDRIVE_FILE_ID"]

def is_parquet(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"

@st.cache_data(show_spinner=True)
def load_data(force: bool = False):
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if (not LOCAL_PATH.exists()) or (not is_parquet(LOCAL_PATH)):
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()

        st.info("Downloading dataset from Google Drive...")
        # gdown handles the 'can't scan for viruses' confirm step automatically
        url = f"https://drive.google.com/uc?id={GDRIVE_FILE_ID}"
        out = gdown.download(url, str(LOCAL_PATH), quiet=False)

        if out is None or not is_parquet(LOCAL_PATH):
            raise RuntimeError("Download failed or file is not a valid Parquet.")

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
