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

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="📚 Catalogo Articoli – AgGrid + Edit", layout="wide")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",  # <-- scrittura
    "https://www.googleapis.com/auth/drive",         # <-- accesso file su Drive
]
REDIRECT_URI = "http://localhost"  # per client "Desktop"

# SOURCE sheet (lettura filtri)
SOURCE_URL = st.secrets["sheet"]["url"]  # es: ...gid=560544700
# DEST sheet (scrittura)
DEST_URL = "https://docs.google.com/spreadsheets/d/1_mwlW5sklv-D_992aWC--S3nfg-OJNOs4Nn2RZr8IPE/edit?gid=405669789#gid=405669789"

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
    return Flow.from_client_config(client_conf, scopes=SCOPES, redirect_uri=REDIRECT_URI)


def get_creds():
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

    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    st.sidebar.info(
        "1) Apri Google e consenti l’accesso\n"
        "2) Verrai reindirizzato a **http://localhost**: copia **l’URL completo** (contiene `code=`)\n"
        "3) Incolla qui sotto l’URL **oppure solo il codice**, poi premi Connetti"
    )
    st.sidebar.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)

    pasted = st.sidebar.text_input("URL completo da http://localhost… **o** solo il codice")
    if st.sidebar.button("✅ Connetti"):
        try:
            raw = pasted.strip()
            if raw.startswith("http"):
                parsed = urlparse(raw)
                code = (parse_qs(parsed.query).get("code") or [None])[0]
            else:
                code = raw
            if not code:
                st.sidebar.error("Non trovo `code`. Incolla l’URL intero o solo il codice.")
                return None
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.sidebar.success("Autenticazione completata ✅")
            return creds
        except Exception as e:
            st.sidebar.error(f"Errore OAuth: {e}")
            return None
    return None


def get_gc(creds_json: dict) -> gspread.Client:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    return gspread.authorize(creds)


@st.cache_data(ttl=300, show_spinner=True)
def load_df(creds_json: dict, sheet_url: str) -> pd.DataFrame:
    gc = get_gc(creds_json)
    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid}.")
    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0) or pd.DataFrame()
    df = df.dropna(how="all")
    # normalizza colonne attese
    for c in ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]:
        if c not in df.columns:
            df[c] = pd.NA
        df[c] = df[c].astype("string").fillna("")
    return df


def load_target_ws(gc: gspread.Client, dest_url: str):
    spreadsheet_id, gid = parse_sheet_url(dest_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet DEST con gid={gid}.")
    return ws


def get_ws_header(ws: gspread.Worksheet):
    # header dalla prima riga
    header = ws.row_values(1)
    return header


def df_from_ws(ws: gspread.Worksheet) -> pd.DataFrame:
    df = get_as_dataframe(ws, evaluate_formulas=False, include_index=False, header=0) or pd.DataFrame()
    df = df.dropna(how="all")
    # garantisci str
    for col in df.columns:
        df[col] = df[col].astype("string").fillna("")
    return df


def upsert_row_by_art_kart(ws: gspread.Worksheet, values_map: dict, key_col="art_kart"):
    """
    Se key esiste -> chiede conferma (via st.session_state) e sovrascrive la riga
    Altrimenti aggiunge una nuova riga in coda.
    """
    header = get_ws_header(ws)
    if key_col not in header:
        raise RuntimeError(f"La colonna chiave '{key_col}' non è presente nell'intestazione del foglio di destinazione.")

    # Mappa i valori nell'ordine delle intestazioni
    row_vals = []
    for h in header:
        v = values_map.get(h, "")
        row_vals.append("" if v is None else str(v))

    # carica df destinazione per verifica esistenza
    df_dest = df_from_ws(ws)
    # Se lo sheet è vuoto (nessuna riga sotto header)
    if df_dest.empty:
        # Append diretto
        ws.append_row(row_vals, value_input_option="USER_ENTERED")
        return "added"

    exists = False
    target_idx = None
    if key_col in df_dest.columns:
        matches = df_dest.index[df_dest[key_col] == str(values_map.get(key_col, ""))].tolist()
        if matches:
            exists = True
            target_idx = matches[0]  # 0-based nel DataFrame (riga dati, non include header)
    else:
        # se l'header non è stato letto correttamente
        raise RuntimeError(f"Il foglio di destinazione non ha la colonna '{key_col}' leggibile.")

    if exists:
        # conferma sovrascrittura
        confirm_key = "confirm_overwrite"
        if not st.session_state.get(confirm_key, False):
            st.warning("⚠️ Record con lo stesso 'art_kart' già presente. Confermi di sovrascrivere?")
            if st.button("Confermo sovrascrittura"):
                st.session_state[confirm_key] = True
                st.experimental_rerun()
            return "await_confirm"
        # calcola row_number su worksheet: header è riga 1, dati partono da 2
        row_number = 2 + target_idx
        cell_range = f"A{row_number}:{chr(ord('A') + len(header) - 1)}{row_number}"
        ws.update(cell_range, [row_vals], value_input_option="USER_ENTERED")
        # reset conferma
        st.session_state.pop(confirm_key, None)
        return "updated"
    else:
        ws.append_row(row_vals, value_input_option="USER_ENTERED")
        return "added"


# -----------------------------
# SIDEBAR: Autenticazione + Filtri
# -----------------------------
st.sidebar.header("🔐 Autenticazione Google")
creds = get_creds()
if not creds:
    st.stop()

# Carica dati sorgente
try:
    df = load_df(json.loads(creds.to_json()), SOURCE_URL)
except Exception as e:
    st.error("❌ Errore caricando il foglio sorgente. Dettagli sotto:")
    st.exception(e)
    st.stop()

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

# -----------------------------
# MAIN LAYOUT: sopra risultati, sotto dettaglio
# -----------------------------
st.subheader("📋 Risultati (seleziona una riga)")
gb = GridOptionsBuilder.from_dataframe(filtered)
gb.configure_selection("single", use_checkbox=True)
gb.configure_grid_options(domLayout="normal")
gb.configure_column("art_kart", header_name="art_kart", pinned="left")
grid_options = gb.build()

grid_resp = AgGrid(
    filtered,
    gridOptions=grid_options,
    height=400,
    data_return_mode="AS_INPUT",
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    fit_columns_on_grid_load=True,
)

selected_rows = grid_resp.get("selected_rows", [])
selected_row = selected_rows[0] if selected_rows else None

st.divider()
st.subheader("🔎 Dettaglio riga selezionata (editabile)")

if not selected_row:
    st.info("Seleziona una riga nella tabella sopra per vedere e modificare il dettaglio qui.")
    st.stop()

# detail editor: tutte le colonne del df (dinamico)
detail_cols = list(df.columns)
# Costruisci un DataFrame 1xN per data_editor
detail_df = pd.DataFrame({c: [selected_row.get(c, "")] for c in detail_cols})

edited_detail = st.data_editor(
    detail_df,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
)

# Pulsante SALVA → scrive su DEST sheet per header columns
st.success("Destinazione di salvataggio: gid=405669789 (stesso file)")
if st.button("💾 Salva su foglio"):
    try:
        # mappa da colonna → valore (string)
        values_map = {c: (str(edited_detail.iloc[0][c]) if c in edited_detail.columns else "") for c in detail_cols}

        # Controllo precondizione: art_kart deve esserci
        art_val = values_map.get("art_kart", "").strip()
        if not art_val:
            st.error("Campo 'art_kart' obbligatorio per salvare.")
            st.stop()

        gc = get_gc(json.loads(Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES).to_json()))
        ws_dest = load_target_ws(gc, DEST_URL)

        result = upsert_row_by_art_kart(ws_dest, values_map, key_col="art_kart")

        if result == "await_confirm":
            st.warning("Conferma richiesta per sovrascrivere. Premi nuovamente 'Confermo sovrascrittura'.")
        elif result == "updated":
            st.success("✅ Riga esistente sovrascritta correttamente.")
            st.cache_data.clear()  # ricarica eventuali viste
        elif result == "added":
            st.success("✅ Nuova riga aggiunta in fondo.")
            st.cache_data.clear()
        else:
            st.info(f"Azione: {result}")
    except Exception as e:
        st.error("❌ Errore durante il salvataggio:")
        st.exception(e)
