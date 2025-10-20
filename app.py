# app.py
import json
import re
from datetime import datetime
from typing import Dict, List

import gspread
import pandas as pd
import streamlit as st
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread.utils import rowcol_to_a1
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# =========================================
# CONFIG BASE
# =========================================
st.set_page_config(page_title="📚 Catalogo Articoli – Edit in-place", layout="wide")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly",
]
REDIRECT_URI = "http://localhost"

SHEET_URL = st.secrets["sheet"]["url"]  # es: https://docs.google.com/spreadsheets/d/XXX/edit

# Colonne che gestiamo con (dropdown + matita + +)
MANAGED_FIELDS = ["Azienda", "Prodotto", "Gradazione", "Annata", "Packaging", "Note"]

# =========================================
# UTILS
# =========================================
def norm_key(x: str) -> str:
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).strip().lower())

def ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def client_from_oauth() -> gspread.Client:
    """
    OAuth installed-app flow. Salva il token in sessione.
    """
    if "gcreds" in st.session_state:
        creds = st.session_state["gcreds"]
    else:
        oauth_cfg = st.secrets["oauth_client"]
        flow = Flow.from_client_config(
            {
                "installed": {
                    "client_id": oauth_cfg["client_id"],
                    "project_id": oauth_cfg["project_id"],
                    "auth_uri": oauth_cfg["auth_uri"],
                    "token_uri": oauth_cfg["token_uri"],
                    "auth_provider_x509_cert_url": oauth_cfg["auth_provider_x509_cert_url"],
                    "client_secret": oauth_cfg["client_secret"],
                    "redirect_uris": oauth_cfg.get("redirect_uris", [REDIRECT_URI]),
                }
            },
            scopes=SCOPES,
        )
        flow.redirect_uri = REDIRECT_URI
        if "auth_code" not in st.session_state:
            auth_url, _ = flow.authorization_url(access_type="offline", prompt="consent")
            st.info("🔐 Autorizza l’accesso a Google e incolla qui il *code*:")
            st.write(auth_url)
            code = st.text_input("Authorization code", type="password")
            if not code:
                st.stop()
            st.session_state["auth_code"] = code
        flow.fetch_token(code=st.session_state["auth_code"])
        creds = flow.credentials
        st.session_state["gcreds"] = creds

    if not isinstance(creds, Credentials):
        creds = Credentials.from_authorized_user_info(json.loads(creds), SCOPES)
    return gspread.authorize(creds)

@st.cache_data(show_spinner=False)
def load_dataframe() -> pd.DataFrame:
    gc = client_from_oauth()
    sh = gc.open_by_url(SHEET_URL)
    # prende il primo foglio (o cambia se necessario)
    ws = sh.sheet1
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    # pulizia righe completamente vuote
    df = df.dropna(how="all")
    df = df.fillna("")
    return df

def write_dataframe(df: pd.DataFrame):
    gc = client_from_oauth()
    sh = gc.open_by_url(SHEET_URL)
    ws = sh.sheet1
    # pulizia NaN → ""
    df = df.fillna("")
    # sovrascrive tutto in modo semplice e robusto
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

def get_unique_map(df: pd.DataFrame) -> Dict[str, List[str]]:
    uniques = {}
    for f in MANAGED_FIELDS:
        if f in df.columns:
            vals = sorted([v for v in df[f].astype(str).unique() if str(v).strip() != ""], key=lambda x: norm_key(x))
        else:
            vals = []
        uniques[f] = vals
    return uniques

def apply_global_rename(df: pd.DataFrame, field: str, old_value: str, new_value: str) -> pd.DataFrame:
    if field not in df.columns:
        return df
    m_old = norm_key(old_value)
    if not new_value.strip():
        return df
    df[field] = df[field].apply(lambda v: new_value if norm_key(v) == m_old else v)
    return df

# =========================================
# SIDEBAR
# =========================================
with st.sidebar:
    st.title("🛠️ Strumenti")
    if st.button("🔄 Aggiorna database", use_container_width=True):
        if "df" in st.session_state:
            try:
                write_dataframe(st.session_state["df"])
                st.success("Database aggiornato con successo.")
            except Exception as e:
                st.error(f"Errore durante il salvataggio: {e}")
        else:
            st.warning("Nessun dato in memoria da salvare.")

    st.divider()
    st.caption("Sessione")
    if st.button("♻️ Ricarica da Google Sheet", use_container_width=True):
        st.cache_data.clear()
        st.session_state.pop("df", None)
        st.rerun()

# =========================================
# DATA LOAD
# =========================================
df = load_dataframe()
df = ensure_cols(df, MANAGED_FIELDS)
st.session_state["df"] = df  # mantieni sempre l’ultima versione in sessione

# mapping valori unici correnti
unique_map = get_unique_map(df)

# =========================================
# LAYOUT
# =========================================
st.title("📚 Catalogo Articoli – Edit in-place")

# ----- TABELLA MASTER -----
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
gb.configure_selection("single", use_checkbox=True)
gb.configure_grid_options(domLayout="normal")
grid = AgGrid(
    df,
    gridOptions=gb.build(),
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    allow_unsafe_jscode=True,
    height=480,
    theme="streamlit",
)

sel = grid.selected_rows
selected_idx = None
if sel and len(sel) > 0:
    # recupera indice vero dal dataframe originale
    # st_aggrid restituisce dict; proviamo a matchare via una chiave robusta
    row_dict = sel[0]
    # se il DF ha un colonna "ID" preferisci quella; altrimenti usa position
    try:
        # posizione nel df originale
        selected_idx = df.index[df.apply(lambda r: all(str(r.get(k, "")) == str(row_dict.get(k, "")) for k in df.columns), axis=1)].tolist()[0]
    except Exception:
        selected_idx = df.index[0]

# =========================================
# DETTAGLIO RIGA (compatto)
# =========================================
st.subheader("🧾 Dettaglio riga selezionata")

if selected_idx is None:
    st.info("Seleziona una riga nella tabella qui sopra.")
    st.stop()

row = df.loc[selected_idx].copy()

# stato per editor campo → mostra input inline su richiesta
if "edit_state" not in st.session_state:
    st.session_state["edit_state"] = {f: {"mode": "select", "value": ""} for f in MANAGED_FIELDS}

def render_compact_field(field_name: str, current_value: str):
    """
    Render: label sinistra, a destra dropdown + icone (matita per rinomina globale, + per aggiungere nuovo)
    Se si attiva la modalità edit/aggiungi, appare input + conferma con modale.
    """
    with st.container():
        col_label, col_field, col_pencil, col_plus = st.columns([0.22, 0.6, 0.09, 0.09])
        with col_label:
            st.markdown(f"**{field_name}**")
        with col_field:
            # dropdown ricercabile (include valore corrente anche se non più nei unici)
            opts = unique_map.get(field_name, [])
            if current_value and current_value not in opts:
                opts = [current_value] + [o for o in opts if norm_key(o) != norm_key(current_value)]
            new_sel = st.selectbox(
                f"{field_name}__select",
                options=opts,
                index=0 if opts and current_value in opts else (0 if opts else None),
                placeholder=f"Seleziona {field_name.lower()}…",
                key=f"{field_name}_select_{selected_idx}",
            )
        with col_pencil:
            if st.button("✏️", key=f"pencil_{field_name}_{selected_idx}", help=f"Rinomina globalmente il valore di {field_name}"):
                st.session_state["edit_state"][field_name]["mode"] = "rename"
                st.session_state["edit_state"][field_name]["value"] = current_value
        with col_plus:
            if st.button("➕", key=f"plus_{field_name}_{selected_idx}", help=f"Aggiungi nuovo valore per {field_name}"):
                st.session_state["edit_state"][field_name]["mode"] = "add"
                st.session_state["edit_state"][field_name]["value"] = ""

        # gestione modalità speciali
        mode = st.session_state["edit_state"][field_name]["mode"]
        if mode in ("rename", "add"):
            default_txt = current_value if mode == "rename" else ""
            txt = st.text_input(
                f"{'Nuovo nome' if mode=='rename' else 'Crea nuovo valore'} per {field_name}",
                value=default_txt,
                key=f"{field_name}_text_{selected_idx}",
                placeholder="Scrivi qui…"
            )

            # calcola impatto rinomina
            affected = 0
            if mode == "rename":
                m = norm_key(current_value)
                affected = int((df[field_name].apply(norm_key) == m).sum())

            col_ok, col_cancel = st.columns([0.2, 0.8])
            with col_ok:
                if st.button("Conferma", key=f"confirm_{field_name}_{selected_idx}"):
                    if mode == "add":
                        # aggiunta semplice: assegna al record selezionato
                        st.session_state["df"].at[selected_idx, field_name] = txt.strip()
                        st.session_state["edit_state"][field_name]["mode"] = "select"
                        st.success(f"Aggiunto nuovo valore per {field_name}: “{txt.strip()}”.")
                        st.rerun()
                    else:
                        # rinomina globale con conferma
                        if not txt.strip():
                            st.warning("Il nuovo valore non può essere vuoto.")
                        else:
                            # Conferma: attenzione, modifichi X righe
                            if "confirm_queue" not in st.session_state:
                                st.session_state["confirm_queue"] = {}
                            st.session_state["confirm_queue"][field_name] = {
                                "old": current_value,
                                "new": txt.strip(),
                                "affected": affected,
                            }
                            st.session_state["edit_state"][field_name]["mode"] = "select"
                            st.session_state["edit_state"][field_name]["value"] = ""
                            st.rerun()
            with col_cancel:
                if st.button("Annulla", key=f"cancel_{field_name}_{selected_idx}"):
                    st.session_state["edit_state"][field_name]["mode"] = "select"
                    st.session_state["edit_state"][field_name]["value"] = ""
                    st.rerun()

        # scrivi subito l’assegnazione del dropdown (singola riga)
        if new_sel != current_value:
            st.session_state["df"].at[selected_idx, field_name] = new_sel

# RENDER CAMPI GESTITI
for f in MANAGED_FIELDS:
    render_compact_field(f, row.get(f, ""))

# RENDER ALTRI CAMPI (semplici, verticali compatti)
st.markdown("### ✍️ Altri campi")
other_cols = [c for c in df.columns if c not in MANAGED_FIELDS]
for c in other_cols:
    val = st.text_input(c, value=str(row.get(c, "")), key=f"free_{c}_{selected_idx}")
    if val != row.get(c, ""):
        st.session_state["df"].at[selected_idx, c] = val

# =========================================
# MODALI DI CONFERMA (rinomina globale)
# =========================================
def show_confirm_modal(field: str, old_v: str, new_v: str, affected: int):
    st.session_state.setdefault("modal_show", True)
    with st.container(border=True):
        st.warning(f"⚠️ Attenzione: stai per rinominare **{field}** \"{old_v}\" → \"{new_v}\" su **{affected}** righe.")
        col_yes, col_no = st.columns([0.25, 0.75])
        with col_yes:
            if st.button("Sì, rinomina", key=f"yes_{field}"):
                st.session_state["df"] = apply_global_rename(st.session_state["df"], field, old_v, new_v)
                st.success(f"Rinominato globalmente {field}: \"{old_v}\" → \"{new_v}\" su {affected} righe.")
                st.session_state["modal_show"] = False
                st.session_state.pop("confirm_queue", None)
                st.rerun()
        with col_no:
            if st.button("No, annulla", key=f"no_{field}"):
                st.session_state["modal_show"] = False
                st.session_state.pop("confirm_queue", None)
                st.info("Operazione annullata.")
                st.rerun()

if "confirm_queue" in st.session_state and st.session_state["confirm_queue"]:
    # Mostra una conferma alla volta
    k, payload = next(iter(st.session_state["confirm_queue"].items()))
    show_confirm_modal(k, payload["old"], payload["new"], payload["affected"])

# =========================================
# FOOTER INFO
# =========================================
st.caption(f"Ultimo aggiornamento locale: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
