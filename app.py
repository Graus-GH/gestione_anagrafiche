# app.py – dettaglio riga più largo, label a sinistra del dropdown, layout super-compatto
import json
import re
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
    auth_url, _ = flow.authorization_url(
        access_type="offline", include_granted_scopes="true", prompt="consent",
    )

    st.sidebar.info("1) Apri Google → consenti\n2) Copia l’URL http://localhost/?code=…\n3) Incollalo qui sotto (o solo il codice) e Connetti")
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
                st.sidebar.error("Non trovo `code`.")
                return None
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.sidebar.success("Autenticazione completata ✅")
            return creds
        except Exception as e:
            msg = str(e)
            if "scope has changed" in msg.lower():
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
    col_vals = ws.col_values(col_idx)  # include header
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
    col_idx = col_map[col_name]  # 1-based
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

# Cache opzioni uniche per tutti i campi SELECT_FIELDS
if "unique_options_by_field" not in st.session_state:
    st.session_state["unique_options_by_field"] = {}

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

# =========================================
# MAIN: SX risultati, DX dettaglio (DX più largo)
# =========================================
left, right = st.columns([1, 1.6], gap="large")  # dettaglio riga allargato

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
        key=f"grid_{st.session_state['data_version']}",
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
    # CSS ultra-compatto: etichette a sinistra, controlli bassi, margini ridotti
    st.markdown(
        """
        <style>
          .row-compact { margin: 2px 0; }
          .labelcell { font-size: 0.86rem; font-weight: 600; padding-top: 6px; }
          div[data-baseweb="select"] > div { min-height: 34px; }
          .stButton button { padding: 0.26rem 0.44rem; min-height: 34px; border-radius: 8px; }
          .stCaption, .stMarkdown p { margin-bottom: 6px !important; }
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
        current_qxc = to_clean_str(full_row.get("QxC", ""))

        # ======= TESTATA =======
        if current_art_desart:
            st.markdown(
                f"<div style='font-size:0.98rem;font-weight:600;line-height:1.25;'>{current_art_desart}</div>",
                unsafe_allow_html=True,
            )

        pill_style = (
            "display:inline-block;background:#eef0f3;border:1px solid #d5d8dc;border-radius:8px;"
            "padding:4px 8px;margin-right:6px;font-size:0.98rem;line-height:1.2;"
        )
        pill_line = "<div style='margin-top:4px;'>"
        if current_art_kart:
            pill_line += f"<span style='{pill_style}'>{current_art_kart}</span>"
        if current_qxc:
            pill_line += f"<span style='{pill_style}'>{current_qxc}</span>"
        pill_line += "</div>"
        st.markdown(pill_line, unsafe_allow_html=True)

        # ======= SUGGERIMENTI SIMILI =======
        try:
            base = df[df["art_kart"].map(to_clean_str) != current_art_kart].copy()
            base["__sim_current__"] = base["art_desart"].apply(lambda s: str_similarity(s, current_art_desart))
            cand = base.sort_values("__sim_current__", ascending=False).head(300).copy()

            labels = [
                f"{to_clean_str(r.get('art_desart',''))} — {to_clean_str(r.get('art_kart',''))} ({sim:.2f})"
                for r, sim in zip(cand.to_dict('records'), cand["__sim_current__"])
            ]
            idx_options = [-1] + list(range(len(cand)))
            label_map = {-1: "— scegli —", **{i: labels[i] for i in range(len(labels))}}

            st.caption("Suggerimenti simili (ordinati per somiglianza, max 300)")
            sc1, sc2 = st.columns([0.88, 0.12])
            with sc1:
                sel_idx = st.selectbox(
                    " ", options=idx_options, index=0, format_func=lambda i: label_map.get(i, str(i)),
                    key=f"simselect_{current_art_kart}", label_visibility="collapsed",
                )
            with sc2:
                st.write("")
                copy_disabled = (sel_idx == -1)
                if st.button(
                    "📋", help="Copia i campi dal selezionato nell’editor (non salva)",
                    disabled=copy_disabled, key=f"btn_copy_{current_art_kart}"
                ):
                    sel_row = cand.iloc[sel_idx].to_dict()
                    prefill = {f: to_clean_str(sel_row.get(f, "")) for f in COPY_FIELDS}

                    if "prefill_by_art_kart" not in st.session_state:
                        st.session_state["prefill_by_art_kart"] = {}
                    st.session_state["prefill_by_art_kart"][current_art_kart] = prefill

                    for field in SELECT_FIELDS:
                        if prefill.get(field):
                            v = normalize_spaces(prefill[field])
                            st.session_state["pending_by_field"][field][current_art_kart] = v
                            st.session_state["selected_by_field"][field][current_art_kart] = v
                            st.session_state["effective_by_field"][field][current_art_kart] = v

                    st.toast("Campi copiati nell'editor. Ricorda di salvare per scrivere sul foglio.", icon="ℹ️")
                    st.rerun()
        except Exception:
            pass

        # =========================
        # Dialog: RINOMINA GLOBALE
        # =========================
        @st.dialog("Rinomina valore globale")
        def dialog_rinomina_generica(col_name: str, old_val: str):
            st.write(f"Colonna: **{col_name}**")
            st.write(f"Valore corrente: **{old_val}**")
            new_val = st.text_input("Nuovo nome", value="", placeholder=f"Nuovo valore per «{col_name}»…")

            X = int((df.get(col_name, pd.Series([], dtype=object)).map(norm_key) == norm_key(old_val)).sum())
            st.warning(f"⚠️ Modificherai **{X}** righe. Confermi?")

            c1, c2 = st.columns(2)
            with c1:
                if st.button("✅ Conferma rinomina", disabled=(normalize_spaces(new_val) == "")):
                    try:
                        creds_json = json.loads(Credentials.from_authorized_user_info(
                            st.session_state["oauth_token"], SCOPES
                        ).to_json())
                        gc = get_gc(creds_json)
                        ws = open_origin_ws(gc)

                        old_clean = normalize_spaces(old_val)
                        new_clean = normalize_spaces(new_val)

                        refresh_unique_cache(col_name)
                        for v in st.session_state["unique_options_by_field"].get(col_name, []):
                            if norm_key(v) == norm_key(new_clean):
                                new_clean = v
                                break

                        changed = batch_find_replace_generic(ws, col_name, old_clean, new_clean)

                        st.cache_data.clear()
                        st.session_state["df"] = load_df(creds_json, SOURCE_URL)
                        for f in SELECT_FIELDS:
                            refresh_unique_cache(f)
                        st.session_state["data_version"] += 1

                        st.session_state["pending_by_field"][col_name][current_art_kart] = new_clean
                        st.session_state["selected_by_field"][col_name][current_art_kart] = new_clean
                        st.session_state["effective_by_field"][col_name][current_art_kart] = new_clean

                        st.success(f"✅ Rinomina completata: {changed} occorrenze aggiornate.")
                        st.toast(f"{col_name}: rinomina globale ok", icon="✅")
                        st.rerun()
                    except Exception as e:
                        st.error("❌ Errore durante la rinomina globale:")
                        st.exception(e)
            with c2:
                if st.button("❌ Annulla"):
                    st.rerun()

        # =========================
        # Dialog: CREA NUOVO valore
        # =========================
        @st.dialog("Crea nuovo valore")
        def dialog_crea_generica(col_name: str, default_text: str = ""):
            candidate = st.text_input("Nuovo valore", value=default_text, placeholder=f"es. nuovo valore per {col_name}")
            st.caption("Il valore verrà aggiunto alla lista e selezionato per la riga corrente.")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("➕ Crea e usa", disabled=(normalize_spaces(candidate) == "")):
                    cand = normalize_spaces(candidate)
                    if all(norm_key(cand) != norm_key(v) for v in st.session_state["unique_options_by_field"].get(col_name, [])):
                        st.session_state["unique_options_by_field"][col_name] = sorted(
                            st.session_state["unique_options_by_field"].get(col_name, []) + [cand],
                            key=lambda x: x.lower()
                        )
                    st.session_state["pending_by_field"][col_name][current_art_kart] = cand
                    st.session_state["selected_by_field"][col_name][current_art_kart] = cand
                    st.session_state["effective_by_field"][col_name][current_art_kart] = cand
                    st.toast(f"✅ Creato nuovo valore per {col_name}: {cand}")
                    st.rerun()
            with c2:
                if st.button("❌ Annulla"):
                    st.rerun()

        # =========================
        # RENDER SELECT COMPACT: label a sinistra + select + ✏️ + ➕
        # =========================
        def render_select_row(col_name: str, full_row, current_art_kart: str):
            current_val = normalize_spaces(full_row.get(col_name, ""))

            pending_map  = st.session_state["pending_by_field"][col_name]
            selected_map = st.session_state["selected_by_field"][col_name]
            effective_map= st.session_state["effective_by_field"][col_name]
            unique_opts  = st.session_state["unique_options_by_field"].get(col_name, [])

            # Valore di default mostrato nel select (senza toccare session_state)
            default_value = (
                effective_map.get(current_art_kart)
                or selected_map.get(current_art_kart)
                or pending_map.get(current_art_kart)
                or current_val
            )

            # Opzioni: tieni tutte le uniche + assicurati che il default sia presente
            options = [""] + unique_opts
            if default_value and all(norm_key(default_value) != norm_key(v) for v in options):
                options.append(default_value)

            # Calcola l'indice del default
            def_idx = next((i for i, opt in enumerate(options) if norm_key(opt) == norm_key(default_value or "")), 0)

            col_label, col_select, col_edit, col_add = st.columns([0.22, 0.58, 0.10, 0.10])
            with col_label:
                st.markdown(f"<div class='labelcell'>{col_name}</div>", unsafe_allow_html=True)

            select_key = f"select_{col_name}_{to_clean_str(current_art_kart)}"
            with col_select:
                val = st.selectbox(
                    " ", options=options, index=def_idx,
                    key=select_key, label_visibility="collapsed"
                )
                val = normalize_spaces(val)

                # Persisti davvero la scelta dell’utente
                selected_map[current_art_kart]  = val
                effective_map[current_art_kart] = val
                pending_map[current_art_kart]   = val

            with col_edit:
                edit_disabled = not bool(val)
                if st.button(
                    "✏️",
                    help=f"Rinomina globalmente il valore selezionato in «{col_name}»",
                    disabled=edit_disabled,
                    key=f"btn_edit_{col_name}_{current_art_kart}"
                ):
                    dialog_rinomina_generica(col_name, val)

            with col_add:
                if st.button(
                    "➕",
                    help=f"Crea un nuovo valore per {col_name}",
                    key=f"btn_add_{col_name}_{current_art_kart}"
                ):
                    dialog_crea_generica(col_name, val)

        # render tutti i campi in pila super-compatta
        for col_name in SELECT_FIELDS:
            with st.container():
                render_select_row(col_name, full_row, current_art_kart)

        # =========================
        # Editor per gli altri campi (esclude i SELECT_FIELDS)
        # =========================
        other_cols = [c for c in WRITE_COLS if c not in SELECT_FIELDS]
        pairs = [{"Campo": c, "Valore": to_clean_str(full_row.get(c, ""))} for c in other_cols]

        prefill_map = (st.session_state.get("prefill_by_art_kart", {}) or {}).get(current_art_kart, {})
        if prefill_map:
            for p in pairs:
                campo = p["Campo"]
                if campo in prefill_map and prefill_map[campo] != "":
                    p["Valore"] = prefill_map[campo]

        detail_table = pd.DataFrame(pairs, columns=["Campo", "Valore"])
        edited_detail = st.data_editor(
            detail_table,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "Campo": st.column_config.TextColumn(disabled=True),
                "Valore": st.column_config.TextColumn(),
            },
            key=f"detail_{to_clean_str(full_row.get('art_kart',''))}_{st.session_state['data_version']}",
        )

        # =========================
        # SALVA
        # =========================
        if st.button("💾 Salva nell'origine"):
            try:
                values_map = {}
                for _, r in edited_detail.iterrows():
                    campo = to_clean_str(r.get("Campo", ""))
                    if campo and campo in other_cols:
                        values_map[campo] = to_clean_str(r.get("Valore", ""))

                eff_all = st.session_state["effective_by_field"]
                sel_all = st.session_state["selected_by_field"]
                pend_all = st.session_state["pending_by_field"]

                for field in SELECT_FIELDS:
                    chosen = eff_all[field].get(current_art_kart)
                    if not chosen:
                        chosen = sel_all[field].get(current_art_kart, "")
                    if not chosen:
                        chosen = pend_all[field].get(current_art_kart, "")
                    if not chosen:
                        ui_key = f"select_{field}_{to_clean_str(current_art_kart)}"
                        chosen = normalize_spaces(st.session_state.get(ui_key, "")) or normalize_spaces(full_row.get(field, ""))
                    values_map[field] = normalize_spaces(chosen)

                art_val = to_clean_str(full_row.get("art_kart", "")) or to_clean_str(values_map.get("art_kart", ""))
                if not art_val:
                    st.error("Campo 'art_kart' obbligatorio.")
                    st.stop()
                values_map["art_kart"] = art_val

                creds_json = json.loads(Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES).to_json())
                gc = get_gc(creds_json)
                spreadsheet_id, gid = parse_sheet_url(SOURCE_URL)
                ws = next((w for w in gc.open_by_key(spreadsheet_id).worksheets() if str(w.id) == str(gid)), None)
                if ws is None:
                    raise RuntimeError(f"Nessun worksheet con gid={gid} nell'origine.")

                art_desart_current = to_clean_str(full_row.get("art_desart", ""))
                result = upsert_in_source(ws, values_map, art_desart_current)

                col_map = ensure_headers(ws, list(dict.fromkeys(WRITE_COLS + ["art_kart"])))
                row_number = find_row_number_by_art_kart_ws(ws, col_map, art_val)

                if row_number is not None:
                    to_force = []
                    for field in SELECT_FIELDS:
                        a1 = rowcol_to_a1(row_number, col_map[field])
                        current_sheet_val = ws.acell(a1).value or ""
                        if normalize_spaces(current_sheet_val) != normalize_spaces(values_map[field]):
                            to_force.append((a1, values_map[field]))
                    for a1, v in to_force:
                        ws.update(a1, [[v]], value_input_option="USER_ENTERED")
                    if to_force:
                        st.info(f"🔧 Aggiornate {len(to_force)} celle con i valori selezionati.")
                else:
                    st.warning("⚠️ Non ho trovato la riga nel foglio dopo il salvataggio. Provo a ricaricare i dati…")

                st.cache_data.clear()
                st.session_state["df"] = load_df(creds_json, SOURCE_URL)
                df = st.session_state["df"]
                for f in SELECT_FIELDS:
                    refresh_unique_cache(f)

                saved_row = df[df["art_kart"].map(to_clean_str) == art_val]
                if not saved_row.empty:
                    for field in SELECT_FIELDS:
                        st.session_state["effective_by_field"][field][current_art_kart] = normalize_spaces(saved_row.iloc[0].get(field, values_map[field]))
                else:
                    for field in SELECT_FIELDS:
                        st.session_state["effective_by_field"][field][current_art_kart] = values_map[field]

                st.session_state["data_version"] += 1

                if "prefill_by_art_kart" in st.session_state and current_art_kart in st.session_state["prefill_by_art_kart"]:
                    st.session_state["prefill_by_art_kart"].pop(current_art_kart, None)

                if row_number is not None:
                    st.success(f"✅ Riga {art_val} aggiornata.")
                else:
                    if result == "updated":
                        st.success(f"✅ Riga {art_val} aggiornata.")
                    elif result == "added":
                        st.success(f"✅ Nuova riga {art_val} aggiunta.")

                st.toast("Salvato!", icon="✅")
                st.rerun()

            except Exception as e:
                st.error("❌ Errore durante il salvataggio:")
                st.exception(e)
