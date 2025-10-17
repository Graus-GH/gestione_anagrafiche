import streamlit as st, os, sys, json
st.title("🔎 Smoke test")

# 1) Python & libs
st.write("Python:", sys.version)
try:
    import pandas, gspread, google_auth_oauthlib
    st.success("✅ Librerie caricate")
except Exception as e:
    st.error(f"❌ Errore librerie: {e}")

# 2) Secrets visibili?
try:
    oc = st.secrets["oauth_client"]
    st.success("✅ Secrets [oauth_client] trovati")
    st.code(json.dumps({"client_id": oc["client_id"][:20]+"…"}, indent=2))
except Exception as e:
    st.error(f"❌ Secrets mancanti/errati: {e}")

try:
    st.write("Sheet URL:", st.secrets["sheet"]["url"])
    st.success("✅ Secrets [sheet] ok")
except Exception as e:
    st.error(f"❌ Secret [sheet] mancante: {e}")

st.info("Se tutto sopra è verde, il problema è nella parte OAuth/spreadsheets. Rimetti l'app completa e guarda i Logs.")
