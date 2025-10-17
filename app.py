import re
import json
import pandas as pd
import streamlit as st
import gspread
from gspread_dataframe import get_as_dataframe
from urllib.parse import urlparse, parse_qs

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request

st.set_page_config(page_title="📊 Catalogo – OAuth", layout="wide")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ==========
# Helpers
# ==========
def parse_sheet_url(sheet_url: str):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("URL del Google Sheet non valido.")
    spreadsheet_id = m.group(1)

    parsed = urlparse(sheet_url)
    gid = None
    if parsed.query:
        q = parse_qs(parsed.query)
        if "gid" in q and len(q["gid"]) > 0:
            gid = q["gid"][0]
    if (not gid) and parsed.fragment and parsed.fragment.startswith("gid="):
        gid = parsed.fragment.split("gid=")[1]
    if gid is None:
        gid = "0"
    return spreadsheet_id, gid

def build_flow():
    client_conf = {
        "installed": {
            "client_id": st.secrets["oauth_client"]["client_id"],
            "project_id": st.secrets["oauth_client"].get("project_id", ""),
            "auth_uri": st.secrets["oauth_client"]["auth_uri"],
            "token_uri": st.secrets["oauth_client"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["oauth_client"]["auth_provider_x509_cert_url"],
            "client_secret": st.secrets["oauth_client"]["client_secret"],
            "redirect_uris": st.secrets["oauth_client"].get("redirect_uris", ["http://localhost"]),
        }
    }
    return Flow.from_client_config(client_conf, scopes=SCOPES)

def get_authorized_creds():
    # 1) token in session?
    if "oauth_token" in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES)
        # refresh se serve
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            st.session_state["oauth_token"] = json.loads(creds.to_json())
        return creds

    # 2) no token: mostra link per login + input code
    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",    # serve refresh_token
        include_granted_scopes="true",
        prompt="consent"          # forza rilascio refresh_token
    )
    st.info("1) Clicca il bottone qui sotto per aprire la pagina di Google, consenti l’accesso e **copia il codice** che Google ti mostra.")
    st.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)

    code = st.text_input("2) Incolla qui il **codice di verifica** fornito da Google", value="", help="Dopo aver concesso l’accesso, Google ti mostra un codice. Copialo e incollalo qui.")
    if st.button("✅ Connetti"):
        try:
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.success("Autenticazione completata! Puoi procedere.")
            return creds
        except Exception as e:
            st.error(f"Errore nell'autenticazione: {e}")
            return None
    return None

@st.cache_data(ttl=300, show_spinner=True)
def load_sheet_as_df(creds_json: dict, sheet_url: str) -> pd.DataFrame:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)

    ws = None
    for w in sh.worksheets():
        if str(w.id) == str(gid):
            ws = w
            break
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid} trovato.")

    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0)
    df = (df if df is not None else pd.DataFrame()).dropna(how="all")

    for col in ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype("string").fillna("")
    return df

# ==========
# UI
# ==========
st.title("📚 Catalogo Articoli – Login Google (OAuth)")

default_url = "https://docs.google.com/spreadsheets/d/1_mwlW5sklv-D_992aWC--S3nfg-OJNOs4Nn2RZr8IPE/edit?gid=560544700#gid=560544700"
sheet_url = st.text_input("URL Google Sheet", value=default_url)

creds = get_authorized_creds()
if not creds:
    st.stop()

try:
    df = load_sheet_as_df(json.loads(creds.to_json()), sheet_url)
except Exception as e:
    st.error(f"Errore nel caricamento del Google Sheet: {e}")
    st.stop()

st.success("✅ Connesso a Google. Dati caricati.")

# ----- Filtri come da tua richiesta -----
with st.expander("Filtri", expanded=True):
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        f_codice = st.text_input("art_kart (codice articolo):", placeholder="es. parte del codice")
        f_desc_boll = st.text_input("art_desart (descrizione Bollicine):", placeholder="testo libero")
    with c2:
        reparti = sorted([v for v in df["art_kmacro"].dropna().unique() if str(v).strip() != ""])
        f_reparti = st.multiselect("art_kmacro (reparto):", options=reparti)
    with c3:
        presenza = st.radio("DescrizioneAffinata presente?", ["Qualsiasi","Presente","Assente"], index=0)
        f_aff_text = st.text_input("Cerca in DescrizioneAffinata:", placeholder="testo libero")

mask = pd.Series(True, index=df.index)
if f_codice.strip():
    mask &= df["art_kart"].str.contains(re.escape(f_codice.strip()), case=False, na=False)
if f_desc_boll.strip():
    mask &= df["art_desart"].str.contains(re.escape(f_desc_boll.strip()), case=False, na=False)
if f_reparti:
    mask &= df["art_kmacro"].isin(f_reparti)
if presenza == "Presente":
    mask &= df["DescrizioneAffinata"].str.strip() != ""
elif presenza == "Assente":
    mask &= df["DescrizioneAffinata"].str.strip() == ""
if f_aff_text.strip():
    mask &= df["DescrizioneAffinata"].str.contains(re.escape(f_aff_text.strip()), case=False, na=False)

filtered = df.loc[mask].copy()
st.markdown(f"**Risultati:** {len(filtered):,}")

cols_show = ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]
st.dataframe(filtered[cols_show], use_container_width=True)

st.download_button(
    "⬇️ Scarica CSV filtrato",
    filtered[cols_show].to_csv(index=False).encode("utf-8"),
    "articoli_filtrati.csv",
    "text/csv",
)
