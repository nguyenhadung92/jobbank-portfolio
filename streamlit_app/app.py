import streamlit as st
import pandas as pd
import requests
from pathlib import Path

st.set_page_config(page_title="Canada Job Bank Dashboard", layout="wide")

st.write("✅ App started")
st.write("✅ Secrets keys:", list(st.secrets.keys()))

# --- MUST exist in Streamlit Secrets ---
# DATA_URL="https://github.com/<user>/<repo>/releases/download/<tag>/jobbank_master.parquet"
try:
    DATA_URL = st.secrets["DATA_URL"]
    st.write("✅ DATA_URL found")
except Exception as e:
    st.error("❌ Missing/invalid secret DATA_URL")
    st.exception(e)
    st.stop()

LOCAL_PATH = Path("jobbank_master.parquet")

def is_parquet(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 4 and p.read_bytes()[:4] == b"PAR1"
    except Exception:
        return False

def download(url: str, dest: Path) -> None:
    st.write("⬇️ Downloading from:", url)
    r = requests.get(url, stream=True, allow_redirects=True, timeout=300)
    st.write("HTTP status:", r.status_code)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    st.write("✅ Downloaded bytes:", dest.stat().st_size)

@st.cache_data(show_spinner=True)
def load_data(force: bool = False) -> pd.DataFrame:
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if (not LOCAL_PATH.exists()) or (not is_parquet(LOCAL_PATH)):
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()
        download(DATA_URL, LOCAL_PATH)

    st.write("✅ Reading parquet...")
    df = pd.read_parquet(LOCAL_PATH)
    st.write("✅ Read done. Shape:", df.shape)
    return df

st.title("🇨🇦 Canada Job Bank – Job Postings Dashboard")

try:
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        df = load_data(force=True)
    else:
        df = load_data()

    st.success(f"Loaded {len(df):,} rows")
    st.dataframe(df.head(100), use_container_width=True)

except Exception as e:
    st.error("❌ App crashed with exception:")
    st.exception(e)
