# app.py
import json
import re
from datetime import datetime
from typing import Dict, List

import gspread
import pandas as pd
import streamlit as st
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
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

# Colonne gestite con dropdown + matita + +
MANAGED_FIELDS = ["Azienda", "Prodotto", "Gradazione", "Annata", "Packaging", "Note"]

HELPER_ROWID = "_rowid__"  # colonna tecnica per tracciare la selezione

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

def _build_flow():
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
    return flow

def client_from_oauth() -> gspread.Client:
    # Nessun widget qui dentro! Legge solo le credenziali salvate
    creds = st.session_state.get("gcreds")
    if creds is None:
        raise RuntimeError("Credenziali Google mancanti in sessione.")
    if isinstance(creds, Credentials):
        pass
    elif isinstance(creds, dict):
        creds = Credentials.from_authorized_user_info(creds, SCOPES)
    elif isinstance(creds, str):
        creds = Credentials.from_authorized_user_info(json.loads(creds), SCOPES)
    return gspread.authorize(creds)

@st.cache_data(show_spinner=False)
def load_dataframe(sheet_url: str) -> pd.DataFrame:
    gc = client_from_oauth()
    sh = gc.open_by_url(sheet_url)
    ws = sh.sheet1
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    df = df.dropna(how="all").fillna("")
    return df

def write_dataframe(df: pd.DataFrame):
    gc = client_from_oauth()
    sh = gc.open_by_url(SHEET_URL)
    ws = sh.sheet1
    # Drop colonna tecnica prima di scrivere
    if HELPER_ROWID in df.columns:
        df = df.drop(columns=[HELPER_ROWID])
    df = df.fillna("")
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

def get_unique_map(df: pd.DataFrame) -> Dict[str, List[str]]:
    uniques = {}
    for f in MANAGED_FIELDS:
        if f in df.columns:
            vals = [v for v in df[f].astype(str).unique() if str(v).strip() != ""]
            vals_sorted = sorted(vals, key=lambda x: norm_key(x))
        else:
            vals_sorted = []
        uniques[f] = vals_sorted
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
        if "df" in st.session_state and isinstance(st.session_state["df"], pd.DataFrame):
            try:
                write_dataframe(st.session_state["df"].copy())
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
# OAUTH UI — fuori da funzioni/cache
# =========================================
if "gcreds" not in st.session_state:
    flow = _build_flow()
    if "auth_url" not in st.session_state:
        auth_url, _ = flow.authorization_url(access_type="offline", prompt="consent")
        st.session_state["auth_url"] = auth_url

    st.info("🔐 Autorizza l’accesso a Google e incolla qui il *code* (solo la stringa di `code=`).")
    st.write(st.session_state["auth_url"])

    code = st.text_input("Authorization code", type="password", key="oauth_code")
    if not code:
        st.stop()

    try:
        flow.fetch_token(code=code)
        st.session_state["gcreds"] = flow.credentials
        st.success("✅ Autorizzazione completata! Carico i dati…")
    except Exception as e:
        st.error(f"Errore OAuth: {e}")
        st.stop()

# =========================================
# DATA LOAD
# =========================================
df = load_dataframe(SHEET_URL)
df = ensure_cols(df, MANAGED_FIELDS)

# Aggiungi colonna tecnica per selezione stabile
df = df.reset_index(drop=True)
df[HELPER_ROWID] = df.index.astype(int)

# Mantieni in sessione
st.session_state["df"] = df

# Mappa valori unici
unique_map = get_unique_map(df)

# =========================================
# LAYOUT
# =========================================
st.title("📚 Catalogo Articoli – Edit in-place")

# ----- TABELLA MASTER -----
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
gb.configure_selection("single", use_checkbox=True)
# Nascondi colonna tecnica
gb.configure_columns({HELPER_ROWID: {"hide": True}})
gb.configure_grid_options(domLayout="normal")

grid = AgGrid(
    df,
    gridOptions=gb.build(),
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    allow_unsafe_jscode=True,
    height=520,
    theme="streamlit",
)

sel = grid.selected_rows
selected_idx = None
if sel and len(sel) > 0:
    try:
        selected_idx = int(sel[0][HELPER_ROWID])
    except Exception:
        selected_idx = 0
else:
    selected_idx = 0 if len(df) else None

# =========================================
# DETTAGLIO RIGA (compatto)
# =========================================
st.subheader("🧾 Dettaglio riga selezionata")

if selected_idx is None:
    st.info("Seleziona una riga nella tabella qui sopra.")
    st.stop()

row = df.loc[selected_idx].copy()

# Stato per editor dei campi
if "edit_state" not in st.session_state:
    st.session_state["edit_state"] = {f: {"mode": "select", "value": ""} for f in MANAGED_FIELDS}

def render_compact_field(field_name: str, current_value: str):
    """
    Layout: label a sinistra, a destra dropdown + icone (matita rinomina globale, + aggiungi nuovo).
    Modalità 'rename' e 'add' mostrano una text input con conferma/annulla.
    """
    with st.container():
        col_label, col_field, col_pencil, col_plus = st.columns([0.22, 0.6, 0.09, 0.09])
        with col_label:
            st.markdown(f"**{field_name}**")
        with col_field:
            opts = unique_map.get(field_name, [])
            # mantieni visibile il valore corrente anche se non è più nei valori unici
            if current_value and all(norm_key(current_value) != norm_key(o) for o in opts):
                opts = [current_value] + [o for o in opts if norm_key(o) != norm_key(current_value)]
            # selettore
            new_sel = st.selectbox(
                f"{field_name}__select",
                options=opts,
                index=0 if current_value in opts else 0 if opts else None,
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

        # Se cambiamo dal dropdown → aggiorna solo la riga selezionata
        if new_sel != current_value:
            st.session_state["df"].at[selected_idx, field_name] = new_sel

        # Modalità rinomina/aggiungi
        mode = st.session_state["edit_state"][field_name]["mode"]
        if mode in ("rename", "add"):
            default_txt = current_value if mode == "rename" else ""
            txt = st.text_input(
                f"{'Nuovo nome' if mode=='rename' else 'Crea nuovo valore'} per {field_name}",
                value=default_txt,
                key=f"{field_name}_text_{selected_idx}",
                placeholder="Scrivi qui…",
            )

            affected = 0
            if mode == "rename":
                m = norm_key(current_value)
                affected = int((st.session_state["df"][field_name].apply(norm_key) == m).sum())

            col_ok, col_cancel = st.columns([0.2, 0.8])
            with col_ok:
                if st.button("Conferma", key=f"confirm_{field_name}_{selected_idx}"):
                    if mode == "add":
                        # Conferma creazione nuovo valore → assegna alla riga corrente
                        val = txt.strip()
                        if not val:
                            st.warning("Il valore non può essere vuoto.")
                        else:
                            st.session_state["df"].at[selected_idx, field_name] = val
                            st.success(f"Aggiunto nuovo valore per {field_name}: “{val}”.")
                            st.session_state["edit_state"][field_name] = {"mode": "select", "value": ""}
                            st.rerun()
                    else:
                        # Rinominare globalmente richiede conferma
                        if not txt.strip():
                            st.warning("Il nuovo valore non può essere vuoto.")
                        else:
                            st.session_state.setdefault("confirm_queue", {})
                            st.session_state["confirm_queue"][field_name] = {
                                "old": current_value,
                                "new": txt.strip(),
                                "affected": affected,
                            }
                            st.session_state["edit_state"][field_name] = {"mode": "select", "value": ""}
                            st.rerun()
            with col_cancel:
                if st.button("Annulla", key=f"cancel_{field_name}_{selected_idx}"):
                    st.session_state["edit_state"][field_name] = {"mode": "select", "value": ""}
                    st.rerun()

# RENDER CAMPI GESTITI
for f in MANAGED_FIELDS:
    render_compact_field(f, row.get(f, ""))

# RENDER ALTRI CAMPI (semplici, verticali compatti)
st.markdown("### ✍️ Altri campi")
other_cols = [c for c in df.columns if c not in MANAGED_FIELDS + [HELPER_ROWID]]
for c in other_cols:
    val_now = str(row.get(c, ""))
    val = st.text_input(c, value=val_now, key=f"free_{c}_{selected_idx}")
    if val != val_now:
        st.session_state["df"].at[selected_idx, c] = val

# =========================================
# MODALE DI CONFERMA — rinomina globale
# =========================================
def show_confirm_modal(field: str, old_v: str, new_v: str, affected: int):
    with st.container(border=True):
        st.warning(
            f"⚠️ Attenzione: stai per rinominare **{field}** "
            f"“{old_v}” → “{new_v}” su **{affected}** righe."
        )
        col_yes, col_no = st.columns([0.25, 0.75])
        with col_yes:
            if st.button("Sì, rinomina", key=f"yes_{field}"):
                st.session_state["df"] = apply_global_rename(st.session_state["df"], field, old_v, new_v)
                st.success(f"Rinominato globalmente {field}: “{old_v}” → “{new_v}” su {affected} righe.")
                st.session_state.pop("confirm_queue", None)
                st.rerun()
        with col_no:
            if st.button("No, annulla", key=f"no_{field}"):
                st.session_state.pop("confirm_queue", None)
                st.info("Operazione annullata.")
                st.rerun()

if "confirm_queue" in st.session_state and st.session_state["confirm_queue"]:
    k, payload = next(iter(st.session_state["confirm_queue"].items()))
    show_confirm_modal(k, payload["old"], payload["new"], payload["affected"])

# =========================================
# FOOTER
# =========================================
st.caption(f"Ultimo aggiornamento locale: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
