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
REDIRECT_URI = "http://localhost"  # per client "Desktop"

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
        gid = (q.get("gid") or [None])[0]
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
    # Forza la redirect per evitare "Missing redirect_uri"
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
                st.warning(f"Refresh token fallito, rifai login. Dettaglio: {e}")
                st.session_state.pop("oauth_token", None)
                return None
        return creds

    # Nessun token: avvia il flow e chiedi l'URL/codice
    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    st.info(
        "1) Clicca **Apri pagina di autorizzazione Google** e consenti l’accesso.\n\n"
        "2) Verrai reindirizzato a **http://localhost** (pagina non raggiungibile): va bene così.\n\n"
        "3) **Copia l’URL completo** dalla barra del browser (inizia con `http://localhost/?code=...`) "
        "oppure incolla solo il **codice** e premi **Connetti**."
    )
    st.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)

    pasted = st.text_input("Incolla l’URL completo da http://localhost… **oppure** solo il codice")
    if st.button("✅ Connetti"):
        try:
            raw = pasted.strip()
            if raw.startswith("http"):
                parsed = urlparse(raw)
                code = (parse_qs(parsed.query).get("code") or [None])[0]
            else:
                code = raw
            if not code:
                st.error("Non trovo `code`. Incolla l’URL intero o solo il codice.")
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
    """Apre il foglio e ritorna un DataFrame normalizzato."""
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)  # se fallisce: API/permessi

    # trova il worksheet col gid richiesto
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid}.")
    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0)
    if df is None:
        df = pd.DataFrame()
    df = df.dropna(how="all")

    # normalizza colonne usate nei filtri
    for c in ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = df[c].astype("string").fillna("")
    return df


# -----------------------------
# UI LAYOUT
# -----------------------------
st.title("📚 Catalogo Articoli – Google OAuth")

SHEET_URL = st.secrets["sheet"]["url"]

# 1) Login
creds = get_creds()
if not creds:
    st.stop()

# 2) Carica dati
try:
    df = load_df(json.loads(creds.to_json()), SHEET_URL)
except Exception as e:
    st.error("❌ Errore caricando il foglio. Dettagli completi sotto:")
    st.exception(e)
    st.stop()

if df.empty:
    st.warning("⚠️ Il foglio è vuoto o non contiene righe leggibili.")
    st.stop()

# -----------------------------
# SIDEBAR (filtri)
# -----------------------------
st.sidebar.header("🎛️ Filtri")
f_code = st.sidebar.text_input("art_kart (codice articolo)", placeholder="es. parte del codice")
f_desc = st.sidebar.text_input("art_desart (descrizione Bollicine)", placeholder="testo libero")

reparti = sorted([v for v in df["art_kmacro"].dropna().unique() if str(v).strip() != ""])
f_reps = st.sidebar.multiselect("art_kmacro (reparto)", reparti)

pres = st.sidebar.radio("DescrizioneAffinata", ["Qualsiasi", "Presente", "Assente"], index=0)
f_aff = st.sidebar.text_input("Cerca in DescrizioneAffinata", placeholder="testo libero")

# Applica filtri
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

filtered = df.loc[mask].copy()

# Colonne principali da mostrare nella griglia top
main_cols = ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]
present_cols = [c for c in main_cols if c in filtered.columns]

# -----------------------------
# MAIN AREA: due sezioni orizzontali
# -----------------------------
top = st.container()
st.divider()
bottom = st.container()

with top:
    st.subheader("📋 Risultati")
    st.caption(f"Righe trovate: **{len(filtered):,}**")

    # Aggiungi colonna di selezione (checkbox singola)
    sel_col = "✓ Seleziona"
    # inizializza la colonna se manca
    if sel_col not in filtered.columns:
        filtered[sel_col] = False

    # se abbiamo una selezione precedente in sessione, ripristinala
    selected_key = st.session_state.get("selected_art_kart")
    if selected_key and selected_key in filtered["art_kart"].values:
        filtered[sel_col] = filtered["art_kart"] == selected_key

    # Mostra editor con checkbox; disabilita editing su altre colonne
    edited = st.data_editor(
        filtered[present_cols + [sel_col]],
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            sel_col: st.column_config.CheckboxColumn(help="Seleziona una riga per vedere i dettagli sotto"),
            "art_kart": st.column_config.TextColumn(disabled=True),
            "art_desart": st.column_config.TextColumn(disabled=True),
            "art_kmacro": st.column_config.TextColumn(disabled=True),
            "DescrizioneAffinata": st.column_config.TextColumn(disabled=True),
        },
        key="results_editor",
    )

    # Enforce selezione singola (tiene la prima spuntata se più di una)
    if sel_col in edited.columns and edited[sel_col].sum() > 1:
        first_idx = edited.index[edited[sel_col]].tolist()[0]
        edited.loc[edited.index != first_idx, sel_col] = False

    # Memorizza la selezione corrente in sessione
    selected_row = None
    if sel_col in edited.columns and edited[sel_col].any():
        selected_row = edited.loc[edited[sel_col]].iloc[0]
        st.session_state["selected_art_kart"] = selected_row.get("art_kart", None)
    else:
        st.session_state.pop("selected_art_kart", None)

    # Download CSV dei risultati
    st.download_button(
        "⬇️ Scarica CSV filtrato",
        edited[present_cols].to_csv(index=False).encode("utf-8"),
        "articoli_filtrati.csv",
        "text/csv",
        use_container_width=True,
    )

with bottom:
    st.subheader("🔎 Dettaglio riga selezionata")
    if selected_row is None:
        st.info("Seleziona una riga nella tabella sopra per vedere il dettaglio qui.")
    else:
        # Recupera il record completo dal df originale tramite art_kart (se presente), altrimenti usa le chiavi dell'edited
        if "art_kart" in df.columns and pd.notna(selected_row.get("art_kart", pd.NA)):
            full_row = df[df["art_kart"] == selected_row["art_kart"]].iloc[0]
        else:
            # fallback: usa l'indice dell'edited
            full_row = df.loc[selected_row.name]

        # Mostra TUTTE le colonne in forma leggibile (key → value)
        detail_df = pd.DataFrame(full_row).reset_index()
        detail_df.columns = ["Campo", "Valore"]
        st.dataframe(detail_df, use_container_width=True, hide_index=True)
