# app.py
import json
import re
from urllib.parse import urlparse, parse_qs
from difflib import SequenceMatcher  # Similarità

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

# Origine (lettura/scrittura) — secrets deve puntare a gid=560544700
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

# Campi copiati dal “simile” (inclusa Azienda)
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
    col_map = ensure_headers(ws, WRITE_COLS)

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

def batch_find_replace_azienda(ws: gspread.Worksheet, old_value: str, new_value: str) -> int:
    col_map = ensure_headers(ws, ["Azienda"])
    col_idx = col_map["Azienda"]  # 1-based
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

def refresh_unique_aziende_cache():
    st.session_state["unique_aziende"] = unique_values_case_insensitive(df["Azienda"]) if "Azienda" in df.columns else []

if "unique_aziende" not in st.session_state:
    refresh_unique_aziende_cache()

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
# MAIN: SX risultati, DX dettaglio (titoli rimossi per spazio)
# =========================================
left, right = st.columns([2, 1], gap="large")

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
        current_azienda = normalize_spaces(full_row.get("Azienda", ""))

        # ======= TESTATA compatta: art_desart + QxC =======
        if current_art_desart:
            badge_qxc = f'<span style="background:#eef0f3;border:1px solid #d5d8dc;border-radius:6px;padding:2px 6px;margin-left:6px;font-size:0.85em;">QxC: {current_qxc or "-"}</span>'
            st.markdown(
                f"<div style='font-size:1.05rem;font-weight:600;line-height:1.25'>{current_art_desart}{badge_qxc}</div>",
                unsafe_allow_html=True
            )

        # =========================
        # CAMPO "Azienda" – select compatta + icon-buttons allineati
        # =========================

        @st.dialog("Rinomina valore «Azienda»")
        def dialog_rinomina_azienda(old_val: str):
            st.write(f"Valore corrente da rinominare: **{old_val}**")
            new_val = st.text_input("Nuovo nome", value="", placeholder="Nuovo nome azienda…")

            X = int((df.get("Azienda", pd.Series([], dtype=object)).map(norm_key) == norm_key(old_val)).sum())
            st.warning(f"⚠️ Stai modificando il valore per **{X}** prodotti/righe. Confermi?")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Conferma rinomina", disabled=(normalize_spaces(new_val) == "")):
                    try:
                        creds_json = json.loads(Credentials.from_authorized_user_info(
                            st.session_state["oauth_token"], SCOPES
                        ).to_json())
                        gc = get_gc(creds_json)
                        ws = open_origin_ws(gc)

                        old_clean = normalize_spaces(old_val)
                        new_clean = normalize_spaces(new_val)

                        unq = unique_values_case_insensitive(df["Azienda"]) if "Azienda" in df.columns else []
                        for v in unq:
                            if norm_key(v) == norm_key(new_clean):
                                new_clean = v
                                break

                        changed = batch_find_replace_azienda(ws, old_clean, new_clean)

                        st.cache_data.clear()
                        st.session_state["df"] = load_df(creds_json, SOURCE_URL)
                        refresh_unique_aziende_cache()
                        st.session_state["data_version"] += 1
                        st.session_state["pending_azienda_value"] = new_clean

                        st.success(f"✅ Rinomina completata: {changed} occorrenze aggiornate.")
                        st.toast("Azienda rinominata globalmente", icon="✅")
                        st.rerun()
                    except Exception as e:
                        st.error("❌ Errore durante la rinomina massiva:")
                        st.exception(e)

            with col2:
                if st.button("❌ Annulla"):
                    st.rerun()

        @st.dialog("Crea nuovo valore «Azienda»")
        def dialog_crea_azienda(default_text: str = ""):
            candidate = st.text_input("Nuovo valore", value=default_text, placeholder="es. Old Group S.p.A.")
            st.caption("Il valore verrà aggiunto alla lista e selezionato per la riga corrente.")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("➕ Crea e usa", disabled=(normalize_spaces(candidate) == "")):
                    cand = normalize_spaces(candidate)
                    if all(norm_key(cand) != norm_key(v) for v in st.session_state.get("unique_aziende", [])):
                        st.session_state["unique_aziende"] = sorted(
                            st.session_state.get("unique_aziende", []) + [cand],
                            key=lambda x: x.lower()
                        )
                    st.session_state["pending_azienda_value"] = cand
                    st.toast(f"✅ Creato nuovo valore: {cand}")
                    st.rerun()
            with col2:
                if st.button("❌ Annulla"):
                    st.rerun()

        unique_aziende = st.session_state.get("unique_aziende", [])
        pending_val = st.session_state.get("pending_azienda_value", None)

        # Chiave del widget selectbox Azienda (così possiamo forzare il reset quando arriva un pending)
        azienda_select_key = f"azienda_select_{to_clean_str(full_row.get('art_kart',''))}_{st.session_state['data_version']}"

        # Se ho un pending da copia/creazione, cancello lo stato del widget per far rispettare l'index nuovo
        if pending_val is not None and azienda_select_key in st.session_state:
            st.session_state.pop(azienda_select_key, None)

        preselect_value = normalize_spaces(pending_val if pending_val is not None else current_azienda)

        options = [""] + unique_aziende
        if preselect_value and all(norm_key(preselect_value) != norm_key(v) for v in options):
            options.append(preselect_value)

        # CSS: riduzione padding per le icon-button
        st.markdown(
            """
            <style>
            .compact-icon-btn button { padding: 0.35rem 0.45rem !important; border-radius: 6px; }
            </style>
            """,
            unsafe_allow_html=True
        )

        c1, c2, c3 = st.columns([0.8, 0.1, 0.1])
        with c1:
            azienda_selected = st.selectbox(
                " ",  # etichetta nascosta
                options=options,
                index=next((i for i, opt in enumerate(options) if norm_key(opt) == norm_key(preselect_value)), 0),
                key=azienda_select_key,
                label_visibility="collapsed",
            )
        with c2:
            edit_disabled = not bool(azienda_selected)
            st.write("")  # micro spacer
            if st.button("✏️", help="Rinomina globalmente il valore selezionato",
                         disabled=edit_disabled,
                         key=f"btn_edit_{st.session_state['data_version']}"):
                dialog_rinomina_azienda(azienda_selected)
        with c3:
            st.write("")
            if st.button("➕", help="Crea un nuovo valore per Azienda",
                         key=f"btn_add_{st.session_state['data_version']}"):
                dialog_crea_azienda("")

        # una volta che il widget è stato renderizzato, consumo il pending (così non resetto ad ogni rerun)
        if pending_val is not None:
            st.session_state.pop("pending_azienda_value", None)

        # ======= SUGGERIMENTI ART_DESART SIMILI (max 300) + copia (icona a destra) =======
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
                    " ",
                    options=idx_options,
                    index=0,
                    format_func=lambda i: label_map.get(i, str(i)),
                    key=f"simselect_{current_art_kart}_{st.session_state['data_version']}",
                    label_visibility="collapsed",
                )

            with sc2:
                st.write("")
                copy_disabled = (sel_idx == -1)
                if st.button("📋", help="Copia i campi dal selezionato nell’editor (non salva)",
                             disabled=copy_disabled,
                             key=f"btn_copy_{current_art_kart}_{st.session_state['data_version']}"):
                    sel_row = cand.iloc[sel_idx].to_dict()
                    prefill = {f: to_clean_str(sel_row.get(f, "")) for f in COPY_FIELDS}

                    # salva prefill degli altri campi
                    if "prefill_by_art_kart" not in st.session_state:
                        st.session_state["prefill_by_art_kart"] = {}
                    st.session_state["prefill_by_art_kart"][current_art_kart] = prefill

                    # preseleziona Azienda nel select dedicato
                    if prefill.get("Azienda"):
                        st.session_state["pending_azienda_value"] = normalize_spaces(prefill["Azienda"])

                    st.toast("Campi copiati nell'editor. Ricorda di salvare per scrivere sul foglio.", icon="ℹ️")
                    st.rerun()
        except Exception:
            pass

        # =========================
        # Editor per gli altri campi (Azienda è sopra)
        # =========================
        other_cols = [c for c in WRITE_COLS if c != "Azienda"]
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

        if st.button("💾 Salva nell'origine"):
            try:
                values_map = {}
                for _, r in edited_detail.iterrows():
                    campo = to_clean_str(r.get("Campo", ""))
                    if campo and campo in other_cols:
                        values_map[campo] = to_clean_str(r.get("Valore", ""))

                # aggiungi Azienda dal selettore (se vuota, preservo quella corrente)
                selected_clean = normalize_spaces(azienda_selected)
                if not selected_clean:
                    selected_clean = current_azienda
                values_map["Azienda"] = selected_clean

                # art_kart obbligatorio
                art_val = to_clean_str(values_map.get("art_kart", ""))
                if not art_val:
                    st.error("Campo 'art_kart' obbligatorio.")
                    st.stop()
                values_map["art_kart"] = art_val

                # client + worksheet origine
                creds_json = json.loads(Credentials.from_authorized_user_info(
                    st.session_state["oauth_token"], SCOPES
                ).to_json())
                gc = get_gc(creds_json)
                spreadsheet_id, gid = parse_sheet_url(SOURCE_URL)
                ws = next((w for w in gc.open_by_key(spreadsheet_id).worksheets() if str(w.id) == str(gid)), None)
                if ws is None:
                    raise RuntimeError(f"Nessun worksheet con gid={gid} nell'origine.")

                # upsert SOLO sulle WRITE_COLS, art_desart_precedente = art_desart attuale
                art_desart_current = to_clean_str(full_row.get("art_desart", ""))
                result = upsert_in_source(ws, values_map, art_desart_current)

                # ✅ aggiorna DB locale (solo WRITE_COLS)
                df_local = st.session_state["df"].copy()
                for c in WRITE_COLS:
                    if c not in df_local.columns:
                        df_local[c] = ""
                values_map["art_desart_precedente"] = art_desart_current

                row_mask = (df_local["art_kart"].map(to_clean_str) == art_val)
                if row_mask.any():
                    idx = df_local.index[row_mask][0]
                    for k in WRITE_COLS:
                        df_local.at[idx, k] = to_clean_str(values_map.get(k, ""))
                else:
                    new_row = {c: "" for c in df_local.columns}
                    for k in WRITE_COLS:
                        new_row[k] = to_clean_str(values_map.get(k, ""))
                    df_local = pd.concat([df_local, pd.DataFrame([new_row])], ignore_index=True)

                # aggiorna cache aziende (idempotenza se nuovo valore)
                st.session_state["df"] = df_local
                if values_map.get("Azienda"):
                    if all(norm_key(values_map["Azienda"]) != norm_key(v) for v in st.session_state.get("unique_aziende", [])):
                        st.session_state["unique_aziende"] = sorted(
                            st.session_state.get("unique_aziende", []) + [values_map["Azienda"]],
                            key=lambda x: x.lower()
                        )
                st.session_state["data_version"] += 1

                # pulisco eventuale prefill usato
                if "prefill_by_art_kart" in st.session_state and current_art_kart in st.session_state["prefill_by_art_kart"]:
                    st.session_state["prefill_by_art_kart"].pop(current_art_kart, None)

                if result == "updated":
                    st.success("✅ Riga aggiornata. UI aggiornata subito.")
                elif result == "added":
                    st.success("✅ Nuova riga aggiunta. UI aggiornata subito.")

                st.toast("Salvato!", icon="✅")
                st.rerun()

            except Exception as e:
                st.error("❌ Errore durante il salvataggio:")
                st.exception(e)
