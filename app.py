# app.py – dettaglio riga più largo, label a sinistra del dropdown, layout super-compatto
import json
import re
import html
from urllib.parse import urlparse, parse_qs
from difflib import SequenceMatcher

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

# Origine (lettura/scrittura)
SOURCE_URL = st.secrets["sheet"]["url"]

# Colonne scrivibili (SOLO queste)
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

# Colonne visibili nei risultati
RESULT_COLS = ["art_kart", "art_desart", "DescrizioneAffinata", "URL_immagine"]

# Campi con select deterministica
SELECT_FIELDS = ["Azienda", "Prodotto", "gradazione", "annata", "Packaging", "Note"]

# Campi copiati dal “simile” (inclusi i select)
COPY_FIELDS = ["Azienda", "Prodotto", "gradazione", "annata", "Packaging", "Note", "URL_immagine"]

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
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        if x.is_integer():
            return str(int(x))
        s = f"{x}"
        return s.rstrip("0").rstrip(".") if "." in s else s
    s = str(x).strip()
    return "" if s.lower() == "nan" else s

def normalize_spaces(s: str) -> str:
    s = to_clean_str(s)
    return " ".join(s.split())

def norm_key(s: str) -> str:
    return normalize_spaces(s).casefold()

def unique_values_case_insensitive(series: pd.Series) -> list[str]:
    d = {}
    for v in series.dropna():
        vv = normalize_spaces(v)
        k = vv.casefold()
        if k and k not in d:
            d[k] = vv
    return sorted(d.values(), key=lambda x: x.lower())

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

def str_similarity(a: str, b: str) -> float:
    a = normalize_spaces(a).lower()
    b = normalize_spaces(b).lower()
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

# --- Diff utilities (word-level) ---
_token_re = re.compile(r"\s+|[^\s]+", re.UNICODE)
def _tokenize_keep_spaces(s: str):
    return _token_re.findall(s or "")
def diff_old_new_html(old: str, new: str) -> tuple[str, str]:
    a = _tokenize_keep_spaces(to_clean_str(old))
    b = _tokenize_keep_spaces(to_clean_str(new))
    sm = SequenceMatcher(a=a, b=b, autojunk=False)
    old_out, new_out = [], []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            old_out.append("".join(html.escape(t) for t in a[i1:i2]))
            new_out.append("".join(html.escape(t) for t in b[j1:j2]))
        elif tag == "delete":
            seg = "".join(html.escape(t) for t in a[i1:i2])
            if seg: old_out.append(f"<span class='diff-del'>{seg}</span>")
        elif tag == "insert":
            seg = "".join(html.escape(t) for t in b[j1:j2])
            if seg: new_out.append(f"<span class='diff-ins'>{seg}</span>")
        elif tag == "replace":
            seg_old = "".join(html.escape(t) for t in a[i1:i2])
            seg_new = "".join(html.escape(t) for t in b[j1:j2])
            if seg_old: old_out.append(f"<span class='diff-del'>{seg_old}</span>")
            if seg_new: new_out.append(f"<span class='diff-ins'>{seg_new}</span>")
    return "".join(old_out), "".join(new_out)

# =========================================
# OAUTH
# =========================================
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
    if st.sidebar.button("🔁 Reset login Google"):
        st.session_state.pop("oauth_token", None)
        st.cache_data.clear()
        st.rerun()
    if "oauth_token" in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                st.session_state["oauth_token"] = json.loads(creds.to_json())
            except Exception:
                st.session_state.pop("oauth_token", None)
                st.warning("Sessione scaduta. Rifai l’accesso.")
                return None
        return creds
    flow = build_flow()
    auth_url, _ = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    st.sidebar.info("1) Apri Google → consenti\n2) Copia l’URL http://localhost/?code=…\n3) Incollalo qui sotto (o solo il codice) e Connetti")
    st.sidebar.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)
    pasted = st.sidebar.text_input("URL completo da http://localhost… **o** solo il codice")
    if st.sidebar.button("✅ Connetti"):
        try:
            raw = pasted.strip()
            code = parse_qs(urlparse(raw).query).get("code", [None])[0] if raw.startswith("http") else raw
            if not code:
                st.sidebar.error("Non trovo `code`.")
                return None
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.sidebar.success("Autenticazione completata ✅")
            return creds
        except Exception as e:
            if "scope has changed" in str(e).lower():
                st.sidebar.warning("Scope cambiati: resetto il login…")
                st.session_state.pop("oauth_token", None)
                st.cache_data.clear()
                st.rerun()
            st.sidebar.error(f"Errore OAuth: {e}")
            return None
    return None

def get_gc(creds_json: dict) -> gspread.Client:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    return gspread.authorize(creds)

# =========================================
# LOAD ORIGINE (lettura)
# =========================================
@st.cache_data(ttl=300, show_spinner=True)
def load_df(creds_json: dict, sheet_url: str) -> pd.DataFrame:
    """Fix: non usare `or` con DataFrame; gestire None esplicitamente."""
    gc = get_gc(creds_json)
    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid}.")
    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0)
    if df is None:
        df = pd.DataFrame()
    elif not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    df = df.dropna(how="all")
    for col in df.columns:
        df[col] = df[col].map(to_clean_str)
    for c in set(RESULT_COLS + WRITE_COLS):
        if c not in df.columns:
            df[c] = ""
        else:
            df[c] = df[c].map(to_clean_str)
    df["art_kart"] = df["art_kart"].map(to_clean_str)
    return df

# =========================================
# SCRITTURA: utilities
# =========================================
def ensure_headers(ws: gspread.Worksheet, required_cols: list[str]) -> dict:
    header = ws.row_values(1) or []
    header = [h if h is not None else "" for h in header]
    norm = [h.strip().lower() for h in header]
    col_map = {}
    changed = False
    for col in required_cols:
        col_norm = col.strip().lower()
        if col_norm in norm:
            idx = norm.index(col_norm) + 1
            col_map[col] = idx
        else:
            header.append(col)
            norm.append(col_norm)
            col_map[col] = len(header)
            changed = True
    if changed:
        rng = f"A1:{rowcol_to_a1(1, len(header))}"
        ws.update(rng, [header], value_input_option="USER_ENTERED")
    return col_map

def find_row_number_by_art_kart_ws(ws: gspread.Worksheet, col_map: dict, art_kart: str) -> int | None:
    col_idx = col_map.get("art_kart")
    if not col_idx:
        return None
    art_val = to_clean_str(art_kart)
    col_vals = ws.col_values(col_idx)
    for i, v in enumerate(col_vals[1:], start=2):
        if to_clean_str(v) == art_val:
            return i
    return None

def upsert_in_source(ws: gspread.Worksheet, values_map: dict, art_desart_current: str) -> str:
    col_map = ensure_headers(ws, list(dict.fromkeys(WRITE_COLS + ["art_kart"])))
    art_val = to_clean_str(values_map.get("art_kart", ""))
    if not art_val:
        raise RuntimeError("Campo 'art_kart' obbligatorio.")
    values_map = {k: to_clean_str(v) for k, v in values_map.items()}
    values_map["art_desart_precedente"] = to_clean_str(art_desart_current)
    row_number = find_row_number_by_art_kart_ws(ws, col_map, art_val)
    if row_number is not None:
        for col in WRITE_COLS:
            c_idx = col_map[col]
            a1 = rowcol_to_a1(row_number, c_idx)
            ws.update(a1, [[to_clean_str(values_map.get(col, ""))]], value_input_option="USER_ENTERED")
        return "updated"
    header = ws.row_values(1) or []
    full_len = len(header)
    new_row = ["" for _ in range(full_len)]
    for col in WRITE_COLS:
        if col in col_map:
            new_row[col_map[col] - 1] = to_clean_str(values_map.get(col, ""))
    ws.append_row(new_row, value_input_option="USER_ENTERED")
    return "added"

def batch_find_replace_generic(ws: gspread.Worksheet, col_name: str, old_value: str, new_value: str) -> int:
    col_map = ensure_headers(ws, [col_name])
    col_idx = col_map[col_name]
    requests = [{
        "findReplace": {
            "find": normalize_spaces(old_value),
            "replacement": normalize_spaces(new_value),
            "matchCase": False,
            "matchEntireCell": True,
            "searchByRegex": False,
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 1,
                "startColumnIndex": col_idx - 1,
                "endColumnIndex": col_idx
            }
        }
    }]
    res = ws.spreadsheet.batch_update({"requests": requests})
    try:
        return int(res["replies"][0]["findReplace"]["occurrencesChanged"])
    except Exception:
        return 0

# =========================================
# APP STATE & DIAGNOSTICA
# =========================================
st.sidebar.header("🔐 Autenticazione Google")
creds = get_creds()
if not creds:
    st.stop()

def get_current_user_email(gc) -> str | None:
    try:
        r = gc.session.get("https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)")
        if r.status_code == 200:
            return r.json().get("user", {}).get("emailAddress")
    except Exception:
        pass
    return None

def open_origin_ws(gc):
    spreadsheet_id, gid = parse_sheet_url(SOURCE_URL)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid}.")
    return ws

with st.sidebar.expander("🧪 Diagnostica scrittura", expanded=False):
    try:
        gc_dbg = get_gc(json.loads(creds.to_json()))
        email = get_current_user_email(gc_dbg)
        st.write("Utente OAuth:", email or "sconosciuto")
        ws_dbg = open_origin_ws(gc_dbg)
        st.write("File:", ws_dbg.spreadsheet.title)
        st.write("Worksheet (gid):", ws_dbg.id)
        if st.button("Prova scrittura (Z1)"):
            from datetime import datetime
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ws_dbg.update("Z1", [[f"TEST {ts}"]], value_input_option="USER_ENTERED")
            st.success("Scrittura di prova riuscita! (cella Z1)")
    except Exception as e:
        st.error(f"Diagnostica: {e}")

# ========= Stato =========
if "data_version" not in st.session_state:
    st.session_state["data_version"] = 0
if "df" not in st.session_state:
    try:
        st.session_state["df"] = load_df(json.loads(creds.to_json()), SOURCE_URL)
    except Exception as e:
        st.error("❌ Errore caricando il foglio (origine).")
        st.exception(e)
        st.stop()
df = st.session_state["df"]

# --- stato vari componenti
st.session_state.setdefault("save_state_by_art", {})
st.session_state.setdefault("last_saved_by_art", {})
st.session_state.setdefault("current_art_kart", None)
st.session_state.setdefault("unique_options_by_field", {})

# === util per ricarica totale ===
def reset_local_state(keep_auth=True):
    keep_keys = {"oauth_token"} if keep_auth else set()
    for k in list(st.session_state.keys()):
        if k not in keep_keys:
            del st.session_state[k]

# Cache opzioni uniche per tutti i campi SELECT_FIELDS
def refresh_unique_cache(field: str):
    if field in df.columns:
        st.session_state["unique_options_by_field"][field] = unique_values_case_insensitive(df[field])
    else:
        st.session_state["unique_options_by_field"][field] = []
for f in SELECT_FIELDS:
    if f not in st.session_state["unique_options_by_field"]:
        refresh_unique_cache(f)

# Mappe pending/selected/effective per campo
def ensure_field_maps():
    if "pending_by_field" not in st.session_state:
        st.session_state["pending_by_field"] = {f:{} for f in SELECT_FIELDS}
    if "selected_by_field" not in st.session_state:
        st.session_state["selected_by_field"] = {f:{} for f in SELECT_FIELDS}
    if "effective_by_field" not in st.session_state:
        st.session_state["effective_by_field"] = {f:{} for f in SELECT_FIELDS}
ensure_field_maps()

# =========================================
# FILTRI
# =========================================
st.sidebar.header("🎛️ Filtri")
f_code = st.sidebar.text_input("art_kart (codice articolo)", placeholder="es. 12345", key="f_code")
f_desc = st.sidebar.text_input("art_desart (descrizione Bollicine)", placeholder="testo libero", key="f_desc")
reparti = sorted([v for v in df.get("art_kmacro", pd.Series([], dtype=object)).dropna().unique() if str(v).strip() != ""])
f_reps = st.sidebar.multiselect("art_kmacro (reparto)", reparti, key="f_reps")
pres = st.sidebar.radio("DescrizioneAffinata", ["Qualsiasi", "Presente", "Assente"], index=0, key="f_pres")
f_aff = st.sidebar.text_input("Cerca in DescrizioneAffinata", placeholder="testo libero", key="f_aff")

# 🔄 Pulsante ricarica dal database: RESET completo dello stato locale
if st.sidebar.button("🔄 Aggiorna dal database"):
    try:
        st.cache_data.clear()
        oauth_backup = st.session_state.get("oauth_token")
        reset_local_state(keep_auth=True)
        if oauth_backup:
            st.session_state["oauth_token"] = oauth_backup
        # ricarica dati freschi
        st.session_state["df"] = load_df(json.loads(Credentials.from_authorized_user_info(oauth_backup, SCOPES).to_json()), SOURCE_URL)
        df = st.session_state["df"]
        st.session_state["data_version"] = 1
        # ricostruisci opzioni
        st.session_state["unique_options_by_field"] = {}
        for f in SELECT_FIELDS:
            refresh_unique_cache(f)
        st.toast("Dati aggiornati dall'origine ✅")
        st.rerun()
    except Exception as e:
        st.sidebar.error("Errore ricaricando i dati:")
        st.sidebar.exception(e)

# Nuovo filtro: Solo Mod? = SI
only_mod_si = st.sidebar.checkbox('Solo Mod? = "SI"', value=False)

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
if only_mod_si and "Mod?" in df.columns:
    mask &= df["Mod?"].map(lambda x: to_clean_str(x).upper() == "SI")

filtered = df.loc[mask].copy()

# =========================================
# MAIN: SX risultati, DX dettaglio (DX più largo)
# =========================================
left, right = st.columns([1, 1.6], gap="large")

with left:
    present_cols = [c for c in RESULT_COLS if c in filtered.columns]
    filtered_results = filtered[present_cols].copy()
    if "art_kart" in filtered_results.columns:
        filtered_results["art_kart"] = filtered_results["art_kart"].map(to_clean_str)

    gb = GridOptionsBuilder.from_dataframe(filtered_results)
    gb.configure_selection("single", use_checkbox=True)
    gb.configure_grid_options(domLayout="normal")
    if "art_kart" in filtered_results.columns:
        gb.configure_column("art_kart", header_name="art_kart", pinned="left")
    grid_options = gb.build()

    grid_resp = AgGrid(
        filtered_results,
        gridOptions=grid_options,
        height=560,
        data_return_mode="AS_INPUT",
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        key=f"grid_{st.session_state.get('data_version',0)}",
    )

    selected_rows = grid_resp.get("selected_rows", [])
    if isinstance(selected_rows, pd.DataFrame):
        selected_rows = selected_rows.to_dict(orient="records")
    elif isinstance(selected_rows, dict):
        selected_rows = [selected_rows]
    elif selected_rows is None:
        selected_rows = []
    elif not isinstance(selected_rows, list):
        try:
            selected_rows = list(selected_rows)
        except Exception:
            selected_rows = []
    selected_row = selected_rows[0] if len(selected_rows) > 0 else None

with right:
    # CSS
    st.markdown(
        """
        <style>
          .row-compact { margin: 2px 0; }
          .labelcell { font-size: 0.86rem; font-weight: 600; padding-top: 6px; }
          div[data-baseweb="select"] > div { min-height: 34px; }
          .stButton button { padding: 0.26rem 0.44rem; min-height: 34px; border-radius: 8px; }
          .stCaption, .stMarkdown p { margin-bottom: 6px !important; }
          .diff-box { background:#fafbfc; border:1px solid #e5e7eb; border-radius:8px; padding:8px 10px; margin-top:8px;}
          .diff-title { font-size:0.85rem; color:#555; margin-bottom:6px;}
          .diff-line { font-size:0.9rem; }
          .diff-label { color:#666; font-weight:600; margin-right:6px; }
          .diff-ins { background:#d9fbe5; text-decoration: underline; }
          .diff-del { background:#fde2e1; text-decoration: line-through; }
          .status-pill { display:inline-flex; align-items:center; gap:6px; padding:6px 12px; border-radius:999px; font-size:0.84rem; border:1px solid; }
          .status-ok  { background:#e6ffed; color:#046a38; border-color:#b7f0c0; }
          .status-dirty{ background:#fff4e5; color:#8a3b00; border-color:#ffd8a8; }
          .dot { width:8px; height:8px; border-radius:50%; display:inline-block; }
          .dot-ok { background:#1f8a4c; }
          .dot-dirty { background:#e86f00; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if selected_row is None:
        st.info("Seleziona una riga nella tabella a sinistra.")
    else:
        # riga completa dall'origine locale
        full_row = None
        if "art_kart" in selected_row and "art_kart" in df.columns:
            key = to_clean_str(selected_row["art_kart"])
            matches = df[df["art_kart"].map(to_clean_str) == key]
            if not matches.empty:
                full_row = matches.iloc[0]
        if full_row is None:
            full_row = pd.Series({c: selected_row.get(c, "") for c in df.columns})

        current_art_kart = to_clean_str(full_row.get("art_kart", ""))
        current_art_desart = to_clean_str(full_row.get("art_desart", ""))
        current_prev_desart = to_clean_str(full_row.get("art_desart_precedente", ""))
        current_qxc = to_clean_str(full_row.get("QxC", ""))
        current_mod_flag = to_clean_str(full_row.get("Mod?", "")).upper()

        # se cambio riga: snapshot base
        if st.session_state["current_art_kart"] != current_art_kart:
            base_snapshot = {}
            for f in SELECT_FIELDS:
                base_snapshot[f] = normalize_spaces(to_clean_str(full_row.get(f, "")))
            for c in [c for c in WRITE_COLS if c not in SELECT_FIELDS]:
                base_snapshot[c] = normalize_spaces(to_clean_str(full_row.get(c, "")))
            st.session_state["last_saved_by_art"][current_art_kart] = base_snapshot
            st.session_state["save_state_by_art"][current_art_kart] = {"just_saved": False}
            st.session_state["current_art_kart"] = current_art_kart

        status_slot = st.empty()  # badge in alto

        # helper valore corrente UI
        def get_current_value(field: str) -> str:
            eff_all = st.session_state.get("effective_by_field", {}).get(field, {})
            sel_all = st.session_state.get("selected_by_field", {}).get(field, {})
            pend_all = st.session_state.get("pending_by_field", {}).get(field, {})
            ui_key = f"select_{field}_{to_clean_str(current_art_kart)}"
            return normalize_spaces(
                st.session_state.get(ui_key, "")
                or eff_all.get(current_art_kart, "")
                or sel_all.get(current_art_kart, "")
                or pend_all.get(current_art_kart, "")
                or full_row.get(field, "")
            )

        # header: descrizione + pillole
        pill_style = (
            "display:inline-block;background:#eef0f3;border:1px solid #d5d8dc;border-radius:8px;"
            "padding:2px 8px;margin-left:6px;font-size:0.86rem;line-height:1.2;"
        )
        header_html = "<div style='display:flex;flex-wrap:wrap;align-items:center;gap:6px;'>"
        if current_art_desart:
            header_html += f"<span style='font-size:0.98rem;font-weight:600;line-height:1.25;'>{current_art_desart}</span>"
        if current_art_kart:
            header_html += f"<span style='{pill_style}'>#{current_art_kart}</span>"
        if current_qxc:
            header_html += f"<span style='{pill_style}'>{current_qxc}</span>"
        header_html += "</div>"
        st.markdown(header_html, unsafe_allow_html=True)

        # linea concat dinamica
        azienda = get_current_value("Azienda")
        prodotto = get_current_value("Prodotto")
        grad = get_current_value("gradazione")
        annata = get_current_value("annata")
        pack = get_current_value("Packaging")
        note = get_current_value("Note")
        parts = []
        if azienda: parts.append(f"{azienda},")
        for v in [prodotto, grad, annata, pack, note]:
            if normalize_spaces(v): parts.append(normalize_spaces(v))
        if current_qxc: parts.append(current_qxc)
        concat_line = " ".join(parts).strip()
        if concat_line:
            st.markdown(f"<div style='color:#444;font-size:0.9rem;margin-top:4px;'>{html.escape(concat_line)}</div>", unsafe_allow_html=True)

        # diff (se Mod?=SI)
        if current_mod_flag == "SI" and (current_prev_desart or current_art_desart):
            old_html, new_html = diff_old_new_html(current_prev_desart, current_art_desart)
            st.markdown(
                f"""
                <div class="diff-box">
                  <div class="diff-title">Differenze descrizione (solo se Mod?=SI)</div>
                  <div class="diff-line"><span class="diff-label">Precedente:</span>{old_html}</div>
                  <div class="diff-line"><span class="diff-label">Attuale:</span>{new_html}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # suggerimenti simili (omesso: invariato rispetto a prima — per brevità tieni la tua ultima versione)

        # dialogs rinomina/crea (omesso: invariato — tieni la tua ultima versione)

        # dropdown + editor (omesso per brevità — tieni la tua ultima versione)
        # ...
        # >>> Qui incolla esattamente le funzioni / blocchi che avevi già (render_select_row, editor, ecc.)
        # L’importante per il bug riportato è il fix di load_df e il reset nel pulsante di ricarica.

        # =========================
        # STATO / SALVA (snapshot)
        # =========================
        # ... (mantieni la tua logica di confronto con last_saved_by_art e l’aggiornamento dello snapshot dopo il salvataggio)
