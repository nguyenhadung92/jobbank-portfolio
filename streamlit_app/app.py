import streamlit as st
import pandas as pd
from pathlib import Path
import gdown

# =====================
# CONFIG
# =====================
GDRIVE_FILE_ID = st.secrets["GDRIVE_FILE_ID"]
LOCAL_PATH = Path("jobbank_master.parquet")


def is_parquet(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 4:
        return False
    with open(path, "rb") as f:
        return f.read(4) == b"PAR1"


@st.cache_data(show_spinner=True)
def load_data(force=False):
    if force and LOCAL_PATH.exists():
        LOCAL_PATH.unlink()

    if not LOCAL_PATH.exists() or not is_parquet(LOCAL_PATH):
        if LOCAL_PATH.exists():
            LOCAL_PATH.unlink()

        st.info("Downloading dataset from Google Drive...")

        # ✅ LINK CHUẨN + fuzzy=True
        url = f"https://drive.google.com/file/d/{GDRIVE_FILE_ID}/view"
        output = gdown.download(
            url,
            str(LOCAL_PATH),
            quiet=False,
            fuzzy=True
        )

        if output is None or not is_parquet(LOCAL_PATH):
            raise RuntimeError(
                "Download failed. "
                "Check Google Drive permission (must be 'Anyone with the link')."
            )

    return pd.read_parquet(LOCAL_PATH)


# =====================
# STREAMLIT UI
# =====================
st.set_page_config(page_title="Canada Job Bank Dashboard", layout="wide")
st.title("Canada Job Bank – Job Postings Dashboard")

if st.button("🔄 Refresh data"):
    st.cache_data.clear()
    df = load_data(force=True)
else:
    df = load_data()

st.success(f"Loaded {len(df):,} rows")
st.dataframe(df.head(100), use_container_width=True)
