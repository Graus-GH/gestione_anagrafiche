# app.py – versione completa con icone e dialoghi moderni
import json
import re
from urllib.parse import urlparse, parse_qs

import gspread
import pandas as pd
import streamlit as st
from gspread_dataframe import get_as_dataframe
from gspread.utils import rowcol_to_a1
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# =========================================
# CONFIG
# =========================================
st.set_page_config(page_title="📚 Catalogo Articoli – Edit in-place", layout="wide")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
REDIRECT_URI = "http://localhost"

SOURCE_URL = st.secrets["sheet"]["url"]

WRITE_COLS = [
    "art_kart",
    "Azienda",
    "Prodotto",
    "gradazione",
    "annata",
    "Packaging",
    "Note",
    "URL_immagine",
    "art_desart_precedente",
]

RESULT_COLS = ["art_kart", "art_desart", "DescrizioneAffinata", "URL_immagine"]

# =========================================
# HELPERS
# =========================================
def to_clean_str(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def normalize_spaces(s: str) -> str:
    return " ".join(to_clean_str(s).split())

def norm_key(s: str) -> str:
    return normalize_spaces(s).casefold()

def unique_values_case_insensitive(series: pd.Series) -> list[str]:
    seen = {}
    for v in series.dropna():
        vv = normalize_spaces(v)
        if vv and norm_key(vv) not in seen:
            seen[norm_key(vv)] = vv
    return sorted(seen.values(), key=str.lower)

def parse_sheet_url(url: str):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        raise ValueError("URL non valido")
    spreadsheet_id = m.group(1)
    parsed = urlparse(url)
    gid = None
    if parsed.query:
        q = parse_qs(parsed.query)
        gid = (q.get("gid") or [None])[0]
    if parsed.fragment and parsed.fragment.startswith("gid="):
        gid = parsed.fragment.split("gid=")[1]
    return spreadsheet_id, gid or "0"

# =========================================
# OAUTH
# =========================================
def build_flow() -> Flow:
    oc = st.secrets["oauth_client"]
    conf = {
        "installed": {
            "client_id": oc["client_id"],
            "project_id": oc.get("project_id", ""),
            "auth_uri": oc["auth_uri"],
            "token_uri": oc["token_uri"],
            "client_secret": oc["client_secret"],
            "redirect_uris": oc.get("redirect_uris", [REDIRECT_URI]),
        }
    }
    return Flow.from_client_config(conf, SCOPES, redirect_uri=REDIRECT_URI)

def get_creds():
    if "oauth_token" in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            st.session_state["oauth_token"] = json.loads(creds.to_json())
        return creds
    flow = build_flow()
    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")
    st.sidebar.link_button("🔐 Apri autorizzazione Google", auth_url)
    code = st.sidebar.text_input("Codice OAuth")
    if st.sidebar.button("Connetti") and code:
        flow.fetch_token(code=code)
        st.session_state["oauth_token"] = json.loads(flow.credentials.to_json())
        st.rerun()
    st.stop()

def get_gc(creds_json: dict) -> gspread.Client:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    return gspread.authorize(creds)

# =========================================
# LOAD SHEET
# =========================================
@st.cache_data(ttl=300)
def load_df(creds_json: dict, sheet_url: str):
    gc = get_gc(creds_json)
    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next(w for w in sh.worksheets() if str(w.id) == str(gid))
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df = df.dropna(how="all")
    for c in df.columns:
        df[c] = df[c].map(to_clean_str)
    return df

def ensure_headers(ws, required_cols):
    header = ws.row_values(1) or []
    header_norm = [h.strip().lower() for h in header]
    mapping = {}
    changed = False
    for c in required_cols:
        cn = c.lower()
        if cn in header_norm:
            mapping[c] = header_norm.index(cn) + 1
        else:
            header.append(c)
            header_norm.append(cn)
            mapping[c] = len(header)
            changed = True
    if changed:
        ws.update("A1", [header])
    return mapping

def upsert_in_source(ws, values_map, art_desart_current):
    col_map = ensure_headers(ws, WRITE_COLS)
    art_val = to_clean_str(values_map["art_kart"])
    row_vals = ws.col_values(col_map["art_kart"])
    row_number = None
    for i, v in enumerate(row_vals[1:], start=2):
        if to_clean_str(v) == art_val:
            row_number = i
            break
    if row_number:
        for c in WRITE_COLS:
            ws.update(rowcol_to_a1(row_number, col_map[c]), [[to_clean_str(values_map.get(c, ""))]])
    else:
        row = ["" for _ in range(len(ws.row_values(1)))]
        for c in WRITE_COLS:
            row[col_map[c]-1] = to_clean_str(values_map.get(c, ""))
        ws.append_row(row)
    return "ok"

def batch_find_replace_azienda(ws, old_value, new_value):
    col_map = ensure_headers(ws, ["Azienda"])
    idx = col_map["Azienda"]
    body = {"requests": [{
        "findReplace": {
            "find": normalize_spaces(old_value),
            "replacement": normalize_spaces(new_value),
            "matchEntireCell": True,
            "matchCase": False,
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 1,
                "startColumnIndex": idx-1,
                "endColumnIndex": idx
            }
        }
    }]}
    res = ws.spreadsheet.batch_update(body)
    return res["replies"][0]["findReplace"].get("occurrencesChanged", 0)

# =========================================
# AUTH
# =========================================
st.sidebar.header("🔐 Login Google")
creds = get_creds()
creds_json = json.loads(creds.to_json())
df = load_df(creds_json, SOURCE_URL)

if "unique_aziende" not in st.session_state:
    st.session_state["unique_aziende"] = unique_values_case_insensitive(df["Azienda"])

def refresh_unique_aziende_cache():
    st.session_state["unique_aziende"] = unique_values_case_insensitive(df["Azienda"])

# =========================================
# DIALOGHI
# =========================================
@st.dialog("Rinomina valore «Azienda»")
def dialog_rinomina_azienda(old_val: str):
    st.write(f"Valore corrente: **{old_val}**")
    new_val = st.text_input("Nuovo nome", value="", placeholder="Nuovo nome azienda…")
    X = int((df["Azienda"].map(norm_key) == norm_key(old_val)).sum())
    st.warning(f"⚠️ Modificherai **{X}** righe.")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("✅ Conferma", disabled=(not new_val.strip())):
            ws = get_gc(creds_json).open_by_key(parse_sheet_url(SOURCE_URL)[0]).worksheet("Sheet1")
            changed = batch_find_replace_azienda(ws, old_val, new_val)
            st.cache_data.clear()
            st.session_state["df"] = load_df(creds_json, SOURCE_URL)
            refresh_unique_aziende_cache()
            st.toast(f"Rinominato {changed} occorrenze", icon="✅")
            st.rerun()
    with c2:
        if st.button("❌ Annulla"):
            st.rerun()

@st.dialog("Crea nuovo valore «Azienda»")
def dialog_crea_azienda(default_text=""):
    candidate = st.text_input("Nuovo valore", value=default_text, placeholder="es. Azienda ABC")
    st.caption("Il valore verrà aggiunto e selezionato automaticamente.")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("➕ Crea", disabled=(not candidate.strip())):
            cand = normalize_spaces(candidate)
            if all(norm_key(cand) != norm_key(v) for v in st.session_state["unique_aziende"]):
                st.session_state["unique_aziende"].append(cand)
                st.session_state["unique_aziende"].sort(key=str.lower)
            st.session_state["pending_azienda_value"] = cand
            st.rerun()
    with c2:
        if st.button("❌ Annulla"):
            st.rerun()

# =========================================
# UI – esempio di dropdown con icone
# =========================================
st.header("Gestione campo «Azienda»")

current = ""
unique = st.session_state["unique_aziende"]
pending = st.session_state.pop("pending_azienda_value", None)
preselect = pending or current
options = [""] + unique
if preselect and preselect not in options:
    options.append(preselect)

col_sel, col_edit, col_add = st.columns([0.8, 0.1, 0.1])

with col_sel:
    azienda_selected = st.selectbox(
        "Azienda",
        options=options,
        index=next((i for i, x in enumerate(options) if norm_key(x) == norm_key(preselect)), 0),
        help="Digita per cercare o selezionare un valore esistente.",
    )

css = """
<style>
div[data-testid="stHorizontalBlock"] button {
  padding: 0.3rem 0.4rem !important;
  border-radius: 6px;
  margin-top: 1.6rem;
}
</style>
"""
st.markdown(css, unsafe_allow_html=True)

with col_edit:
    disabled = not bool(azienda_selected)
    if st.button("✏️", help="Rinomina globalmente il valore selezionato", disabled=disabled):
        dialog_rinomina_azienda(azienda_selected)

with col_add:
    if st.button("➕", help="Aggiungi un nuovo valore"):
        dialog_crea_azienda("")
