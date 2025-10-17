# app.py
import json
import re
from urllib.parse import urlparse, parse_qs

import gspread
import pandas as pd
import streamlit as st
from gspread_dataframe import get_as_dataframe
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="📚 Catalogo Articoli – Google OAuth", layout="wide")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
REDIRECT_URI = "http://localhost"  # obbligatoria per client "Desktop"

# -----------------------------
# UTILS
# -----------------------------
def parse_sheet_url(url: str):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        raise ValueError("URL Google Sheet non valido.")
    spreadsheet_id = m.group(1)

    parsed = urlparse(url)
    gid = None
    if parsed.query:
        q = parse_qs(parsed.query)
        if "gid" in q and q["gid"]:
            gid = q["gid"][0]
    if (not gid) and parsed.fragment and parsed.fragment.startswith("gid="):
        gid = parsed.fragment.split("gid=")[1]
    return spreadsheet_id, (gid or "0")


def build_flow() -> Flow:
    oc = st.secrets["oauth_client"]
    client_conf = {
        "installed": {
            "client_id": oc["client_id"],
            "project_id": oc.get("project_id", ""),
            "auth_uri": oc["auth_uri"],
            "token_uri": oc["token_uri"],
            "auth_provider_x509_cert_url": oc["auth_provider_x509_cert_url"],
            "client_secret": oc["client_secret"],
            "redirect_uris": oc.get("redirect_uris", [REDIRECT_URI]),
        }
    }
    # forza redirect_uri (evita "Missing redirect_uri")
    return Flow.from_client_config(client_conf, scopes=SCOPES, redirect_uri=REDIRECT_URI)


def get_creds():
    # Token già presente?
    if "oauth_token" in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                st.session_state["oauth_token"] = json.loads(creds.to_json())
            except Exception as e:
                st.warning(f"⚠️ Refresh token fallito, rifai login. Dettaglio: {e}")
                st.session_state.pop("oauth_token", None)
                return None
        return creds

    # Nessun token: avvia il flow e chiedi l'URL incollato
    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    st.info(
        "1) Clicca **Apri pagina di autorizzazione Google** e consenti l’accesso.\n\n"
        "2) Verrai reindirizzato a **http://localhost** (pagina non raggiungibile): va bene così.\n\n"
        "3) **Copia l’URL completo** dalla barra del browser (inizia con `http://localhost/?code=...`) e incollalo qui sotto."
    )
    st.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)

    pasted = st.text_input("Incolla qui l’**URL completo** da `http://localhost/?code=...`")
    if st.button("✅ Connetti"):
        try:
            parsed = urlparse(pasted.strip())
            code = (parse_qs(parsed.query).get("code") or [None])[0]
            if not code:
                st.error("URL non valido: non trovo `code=`. Incolla l’URL INTERO dalla barra del browser.")
                return None
            flow.fetch_token(code=code)  # scambio codice → token
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.success("Autenticazione completata ✅")
            return creds
        except Exception as e:
            st.error(f"Errore OAuth: {e}")
            return None
    return None


@st.cache_data(ttl=300, show_spinner=True)
def load_df(creds_json: dict, sheet_url: str) -> pd.DataFrame:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)

    # trova il worksheet col gid richiesto
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid}")

    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0) or pd.DataFrame()
    df = df.dropna(how="all")

    # normalizza colonne usate nei filtri
    for c in ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = df[c].astype("string").fillna("")

    return df


# -----------------------------
# UI
# -----------------------------
st.title("📚 Catalogo Articoli – Google OAuth")

# URL del foglio dai Secrets (puoi renderlo editabile se ti serve)
SHEET_URL = st.secrets["sheet"]["url"]

# 1) Login
creds = get_creds()
if not creds:
    st.stop()

# 2) Carica dati
try:
    df = load_df(json.loads(creds.to_json()), SHEET_URL)
except Exception as e:
    st.error(f"Errore caricando il foglio: {e}")
    st.stop()

st.success("✅ Connesso a Google e dati caricati.")

# 3) Filtri
with st.expander("Filtri", expanded=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        f_code = st.text_input("art_kart (codice articolo)", placeholder="es. parte del codice")
        f_desc = st.text_input("art_desart (descrizione Bollicine)", placeholder="testo libero")
    with c2:
        reparti = sorted([v for v in df["art_kmacro"].dropna().unique() if str(v).strip() != ""])
        f_reps = st.multiselect("art_kmacro (reparto)", reparti)
    with c3:
        pres = st.radio("DescrizioneAffinata", ["Qualsiasi", "Presente", "Assente"], index=0)
        f_aff = st.text_input("Cerca in DescrizioneAffinata", placeholder="testo libero")

mask = pd.Series(True, index=df.index)
if f_code.strip():
    mask &= df["art_kart"].str.contains(re.escape(f_code.strip()), case=False, na=False)
if f_desc.strip():
    mask &= df["art_desart"].str.contains(re.escape(f_desc.strip()), case=False, na=False)
if f_reps:
    mask &= df["art_kmacro"].isin(f_reps)
if pres == "Presente":
    mask &= df["DescrizioneAffinata"].str.strip() != ""
elif pres == "Assente":
    mask &= df["DescrizioneAffinata"].str.strip() == ""
if f_aff.strip():
    mask &= df["DescrizioneAffinata"].str.contains(re.escape(f_aff.strip()), case=False, na=False)

out = df.loc[mask, ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]]
st.markdown(f"**Risultati:** {len(out):,}")
st.dataframe(out, use_container_width=True)

st.download_button(
    "⬇️ Scarica CSV filtrato",
    out.to_csv(index=False).encode("utf-8"),
    "articoli_filtrati.csv",
    "text/csv",
)
