# app.py
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
    "https://www.googleapis.com/auth/drive.readonly",
]
REDIRECT_URI = "http://localhost"
SOURCE_URL = st.secrets["sheet"]["url"]

WRITE_COLS = [
    "art_kart", "Azienda", "Prodotto", "gradazione", "annata",
    "Packaging", "Note", "URL_immagine", "art_desart_precedente"
]
RESULT_COLS = ["art_kart", "art_desart", "DescrizioneAffinata", "URL_immagine"]

# =========================================
# HELPERS
# =========================================
def to_clean_str(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def normalize_spaces(s): return " ".join(to_clean_str(s).split())
def norm_key(s): return normalize_spaces(s).casefold()

def unique_values_case_insensitive(series):
    d = {}
    for v in series.dropna():
        vv = normalize_spaces(v)
        k = vv.casefold()
        if k and k not in d:
            d[k] = vv
    return sorted(d.values(), key=lambda x: x.lower())

def parse_sheet_url(url):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m: raise ValueError("URL Google Sheet non valido.")
    spreadsheet_id = m.group(1)
    parsed = urlparse(url)
    gid = None
    if parsed.query:
        q = parse_qs(parsed.query)
        gid = (q.get("gid") or [None])[0]
    if (not gid) and parsed.fragment and parsed.fragment.startswith("gid="):
        gid = parsed.fragment.split("gid=")[1]
    return spreadsheet_id, (gid or "0")

# =========================================
# OAUTH
# =========================================
def build_flow():
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
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            st.session_state["oauth_token"] = json.loads(creds.to_json())
        return creds

    flow = build_flow()
    auth_url, _ = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    st.sidebar.link_button("🔐 Autorizza Google", auth_url)
    pasted = st.sidebar.text_input("Incolla l’URL http://localhost… o solo il codice:")
    if st.sidebar.button("✅ Connetti"):
        code = pasted.strip()
        if code.startswith("http"):
            parsed = urlparse(code)
            code = (parse_qs(parsed.query).get("code") or [None])[0]
        flow.fetch_token(code=code)
        creds = flow.credentials
        st.session_state["oauth_token"] = json.loads(creds.to_json())
        st.sidebar.success("✅ Login completato")
        return creds
    return None

def get_gc(creds_json): return gspread.authorize(Credentials.from_authorized_user_info(creds_json, SCOPES))

# =========================================
# DATA
# =========================================
@st.cache_data(ttl=300)
def load_df(creds_json, sheet_url):
    gc = get_gc(creds_json)
    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df = df.dropna(how="all")
    for col in df.columns: df[col] = df[col].map(to_clean_str)
    for c in set(RESULT_COLS + WRITE_COLS):
        if c not in df.columns: df[c] = ""
    return df

def open_origin_ws(gc):
    spreadsheet_id, gid = parse_sheet_url(SOURCE_URL)
    sh = gc.open_by_key(spreadsheet_id)
    return next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)

def batch_find_replace_azienda(ws, old, new):
    col_idx = 2  # supponiamo che la colonna "Azienda" sia la seconda (aggiorna se diverso)
    req = [{
        "findReplace": {
            "find": normalize_spaces(old),
            "replacement": normalize_spaces(new),
            "matchCase": False,
            "matchEntireCell": True,
            "range": {"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": col_idx - 1, "endColumnIndex": col_idx}
        }
    }]
    res = ws.spreadsheet.batch_update({"requests": req})
    try:
        return int(res["replies"][0]["findReplace"]["occurrencesChanged"])
    except Exception:
        return 0

# =========================================
# APP
# =========================================
st.sidebar.header("🔐 Autenticazione Google")
creds = get_creds()
if not creds:
    st.stop()

if "df" not in st.session_state:
    st.session_state["df"] = load_df(json.loads(creds.to_json()), SOURCE_URL)
if "data_version" not in st.session_state:
    st.session_state["data_version"] = 0

df = st.session_state["df"]

if "unique_aziende" not in st.session_state:
    st.session_state["unique_aziende"] = unique_values_case_insensitive(df["Azienda"])

def refresh_unique_aziende_cache():
    st.session_state["unique_aziende"] = unique_values_case_insensitive(df["Azienda"])

# =========================================
# UI
# =========================================
st.subheader("📋 Catalogo Articoli")
left, right = st.columns([2, 1])

with left:
    gb = GridOptionsBuilder.from_dataframe(df[["art_kart", "Azienda", "Prodotto"]])
    gb.configure_selection("single")
    grid = AgGrid(df, gridOptions=gb.build(), update_mode=GridUpdateMode.SELECTION_CHANGED)
    sel = grid.get("selected_rows", [])
    sel = sel[0] if sel else None

with right:
    if not sel:
        st.info("Seleziona una riga per modificare.")
        st.stop()

    full_row = df[df["art_kart"] == sel["art_kart"]].iloc[0]

    # Dialog RINOMINA (precompilato)
    @st.dialog("Rinomina valore «Azienda»")
    def dialog_rinomina_azienda(old_val: str):
        old_clean = normalize_spaces(old_val)
        new_val = st.text_input("Nuovo nome", value=old_clean, placeholder="Nuovo nome azienda…")

        X = int((df["Azienda"].map(norm_key) == norm_key(old_clean)).sum())
        st.warning(f"⚠️ Stai modificando il valore per **{X}** prodotti/righe. Confermi?")

        new_clean = normalize_spaces(new_val)
        disable_confirm = (new_clean == "") or (norm_key(new_clean) == norm_key(old_clean))

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Conferma rinomina", disabled=disable_confirm):
                creds_json = json.loads(Credentials.from_authorized_user_info(
                    st.session_state["oauth_token"], SCOPES).to_json())
                gc = get_gc(creds_json)
                ws = open_origin_ws(gc)
                changed = batch_find_replace_azienda(ws, old_clean, new_clean)
                st.cache_data.clear()
                st.session_state["df"] = load_df(creds_json, SOURCE_URL)
                refresh_unique_aziende_cache()
                st.session_state["pending_azienda_value"] = new_clean
                st.session_state["data_version"] += 1
                st.success(f"✅ Rinomina completata: {changed} occorrenze aggiornate.")
                st.toast("Azienda rinominata globalmente", icon="✅")
                st.rerun()

        with col2:
            if st.button("❌ Annulla"): st.rerun()

    # Dialog CREA nuovo valore
    @st.dialog("Crea nuovo valore «Azienda»")
    def dialog_crea_azienda(default_text=""):
        candidate = st.text_input("Nuovo valore", value=default_text)
        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ Crea e usa", disabled=(normalize_spaces(candidate) == "")):
                cand = normalize_spaces(candidate)
                if all(norm_key(cand) != norm_key(v) for v in st.session_state["unique_aziende"]):
                    st.session_state["unique_aziende"].append(cand)
                    st.session_state["unique_aziende"].sort(key=lambda x: x.lower())
                st.session_state["pending_azienda_value"] = cand
                st.toast(f"✅ Creato nuovo valore: {cand}")
                st.rerun()
        with col2:
            if st.button("❌ Annulla"): st.rerun()

    # Campo Azienda
    current_azienda = normalize_spaces(full_row["Azienda"])
    pending_val = st.session_state.pop("pending_azienda_value", None)
    preselect_value = normalize_spaces(pending_val or current_azienda)
    options = [""] + st.session_state["unique_aziende"]
    if preselect_value not in options:
        options.append(preselect_value)

    col_sel, col_edit, col_add = st.columns([0.8, 0.1, 0.1])
    with col_sel:
        st.markdown("**Azienda**")
        azienda_selected = st.selectbox(
            "Seleziona o cerca",
            options=options,
            index=options.index(preselect_value) if preselect_value in options else 0,
            key=f"azienda_{st.session_state['data_version']}",
        )

    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] button {
        padding: 0.3rem 0.4rem !important;
        border-radius: 6px;
        margin-top: 1.6rem;
    }
    </style>
    """, unsafe_allow_html=True)

    with col_edit:
        if st.button("✏️", help="Rinomina globalmente", disabled=not azienda_selected):
            dialog_rinomina_azienda(azienda_selected)

    with col_add:
        if st.button("➕", help="Crea nuovo valore"):
            dialog_crea_azienda("")

    st.divider()
    st.text_input("Prodotto", value=full_row["Prodotto"])
    st.text_area("Note", value=full_row["Note"])
    st.image(full_row["URL_immagine"], use_column_width=True, caption="Immagine (se disponibile)")
