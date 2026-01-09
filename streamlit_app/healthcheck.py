import streamlit as st
import requests

st.set_page_config(page_title="Healthcheck", layout="wide")
st.title("✅ Jobbank App Healthcheck")

DATA_URL = st.secrets.get("DATA_URL", None)

st.write("Has DATA_URL secret:", DATA_URL is not None)
if DATA_URL:
    st.code(DATA_URL)

    try:
        st.subheader("1) HEAD request")
        h = requests.head(DATA_URL, allow_redirects=True, timeout=60)
        st.write("HEAD status:", h.status_code)
        st.write("Final URL:", h.url)
        st.write("Content-Type:", h.headers.get("Content-Type"))
        st.write("Content-Length:", h.headers.get("Content-Length"))

        st.subheader("2) GET first bytes (range)")
        g = requests.get(
            DATA_URL,
            headers={"Range": "bytes=0-200"},
            allow_redirects=True,
            timeout=60,
        )
        st.write("GET status:", g.status_code)
        st.write("Final URL:", g.url)
        st.write("Content-Type:", g.headers.get("Content-Type"))
        st.write("First bytes:", g.content[:80])

    except Exception as e:
        st.error("Request failed:")
        st.exception(e)
else:
    st.warning("Set DATA_URL in Streamlit Secrets first.")
