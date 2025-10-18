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

# =========================================
# HELPERS
# =========================================
def to_clean_str(x):
    """Converte in stringa pulita (no .0 sugli interi, no 'nan')."""
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
    """Collassa spazi multipli e trim."""
    s = to_clean_str(s)
    return " ".join(s.split())

def norm_key(s: str) -> str:
    """Chiave di normalizzazione case-insensitive + trim + spazi."""
    return normalize_spaces(s).casefold()

def unique_values_case_insensitive(series: pd.Series) -> list[str]:
    """Valori unici per 'Azienda' in modo case-insensitive e con spazi normalizzati."""
    d = {}
    for v in series.dropna():
        vv = normalize_spaces(v)
        k = vv.casefold()
        if k and k not in d:
            d[k] = vv
    # ordina alfabeticamente mantenendo display "pulito"
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

    # pulizia stringhe
    for col in df.columns:
        df[col] = df[col].map(to_clean_str)

    # garantisci colonne per UI/scrittura
    for c in set(RESULT_COLS + WRITE_COLS):
        if c not in df.columns:
            df[c] = ""
        else:
            df[c] = df[c].map(to_clean_str)

    df["art_kart"] = df["art_kart"].map(to_clean_str)
    return df

# =========================================
# SCRITTURA: utilities (robuste)
# =========================================
def ensure_headers(ws: gspread.Worksheet, required_cols: list[str]) -> dict:
    """
    Ritorna mappa {col: idx(1-based)}. Cerca le intestazioni in modo case-insensitive e trim;
    se non trova la colonna la aggiunge in coda col nome esatto richiesto.
    """
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
    """Trova la riga reale nel foglio cercando art_kart nella sua colonna (match esatto)."""
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
    """
    Scrive SOLO WRITE_COLS nell’origine:
    - se art_kart esiste → sovrascrive (senza conferma)
    - altrimenti appende una nuova riga con solo le WRITE_COLS
    """
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
    """
    Rinomina massivamente il valore di 'Azienda' usando Sheets batchUpdate/findReplace
    con match dell'intera cella e case-insensitive, limitato alla sola colonna 'Azienda'.
    Ritorna il numero di occorrenze modificate secondo la risposta API.
    """
    # assicura la colonna e ottieni indice 1-based
    col_map = ensure_headers(ws, ["Azienda"])
    col_idx = col_map["Azienda"]  # 1-based
    # range limitato alla colonna 'Azienda' (tutte le righe)
    requests = [{
        "findReplace": {
            "find": normalize_spaces(old_value),
            "replacement": normalize_spaces(new_value),
            "matchCase": False,
            "matchEntireCell": True,
            "searchByRegex": False,
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 1,             # esclude header
                "startColumnIndex": col_idx - 1,
                "endColumnIndex": col_idx
            }
        }
    }]
    res = ws.spreadsheet.batch_update({"requests": requests})
    # la risposta contiene replies.findReplace.occurrencesChanged
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

# cache dei valori unici per Azienda
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
reparti = sorted([v for v in df.get("art_kmacro", pd.Series([])).dropna().unique() if str(v).strip() != ""])
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
# MAIN: SX risultati, DX dettaglio
# =========================================
left, right = st.columns([2, 1], gap="large")

with left:
    st.subheader("📋 Risultati")
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
    st.subheader("🔎 Dettaglio riga selezionata (editabile)")
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

        # =========================
        # Selettore intelligente Azienda
        # =========================
        current_azienda = normalize_spaces(full_row.get("Azienda", ""))
        unique_aziende = st.session_state.get("unique_aziende", [])
        # Pre-seleziona se presente (case-insensitive)
        selected_idx = 0
        options = [""] + unique_aziende  # "" = nessuna/valore vuoto
        if current_azienda:
            for i, opt in enumerate(options):
                if norm_key(opt) == norm_key(current_azienda):
                    selected_idx = i
                    break

        st.markdown("**Azienda**")
        azienda_selected = st.selectbox(
            "Seleziona o cerca",
            options=options,
            index=selected_idx,
            help="Digita per filtrare (type-ahead).",
            key=f"azienda_select_{to_clean_str(full_row.get('art_kart',''))}_{st.session_state['data_version']}"
        )

        st.caption("Oppure inserisci un nuovo valore:")
        new_azienda_input = st.text_input(
            "Nuovo valore (crea se non esiste)",
            value="",
            placeholder="es. Old Group S.p.A.",
            label_visibility="collapsed",
            key=f"azienda_new_{to_clean_str(full_row.get('art_kart',''))}_{st.session_state['data_version']}"
        )

        colA, colB, colC = st.columns([1, 1, 1])
        with colA:
            if st.button(f"➕ Crea e usa «{normalize_spaces(new_azienda_input)}»", disabled=(normalize_spaces(new_azienda_input) == "")):
                candidate = normalize_spaces(new_azienda_input)
                # idempotenza: se già esiste (case-insensitive) seleziona quello
                exists = any(norm_key(candidate) == norm_key(v) for v in unique_aziende)
                if not exists:
                    # aggiungi in cache locale
                    st.session_state["unique_aziende"] = sorted(unique_aziende + [candidate], key=lambda x: x.lower())
                # imposta selezione corrente
                azienda_selected = candidate
                st.session_state[f"azienda_select_{to_clean_str(full_row.get('art_kart',''))}_{st.session_state['data_version']}"] = candidate
                st.success(f"Creato (in cache) e selezionato: {candidate}")

        with colB:
            # conteggio righe che hanno l'attuale valore selezionato (non vuoto)
            can_mass_rename = bool(azienda_selected and norm_key(azienda_selected) != norm_key(normalize_spaces(new_azienda_input)))
            if st.button("✏️ Cambia valore globale", disabled=not can_mass_rename):
                st.session_state["open_mass_rename"] = True

        with colC:
            st.write("")  # spacer

        # Dialog per rinomina massiva
        if st.session_state.get("open_mass_rename"):
            with st.modal("Rinomina massiva «Azienda»", key="rename_modal"):
                old_val = azienda_selected
                st.write(f"Valore corrente da rinominare: **{old_val}**")
                new_val = st.text_input("Nuovo nome", value="", placeholder="Nuovo nome azienda…")
                # Conteggio X
                X = int((df.get("Azienda", pd.Series([])).map(norm_key) == norm_key(old_val)).sum())
                st.warning(f"Attenzione, stai modificando il valore per **{X}** prodotti/righe. Confermi?")
                c1, c2 = st.columns([1, 1])
                with c1:
                    confirm = st.button("✅ Conferma rinomina globale", disabled=(normalize_spaces(new_val) == ""))
                with c2:
                    cancel = st.button("Annulla")

                if cancel:
                    st.session_state["open_mass_rename"] = False
                    st.rerun()

                if confirm:
                    try:
                        creds_json = json.loads(Credentials.from_authorized_user_info(
                            st.session_state["oauth_token"], SCOPES
                        ).to_json())
                        gc = get_gc(creds_json)
                        ws = open_origin_ws(gc)

                        old_clean = normalize_spaces(old_val)
                        new_clean = normalize_spaces(new_val)

                        # Idempotenza: se il nuovo valore è equivalente ad un esistente, usa il display già presente
                        unq = unique_values_case_insensitive(df["Azienda"]) if "Azienda" in df.columns else []
                        for v in unq:
                            if norm_key(v) == norm_key(new_clean):
                                new_clean = v
                                break

                        changed = batch_find_replace_azienda(ws, old_clean, new_clean)

                        # refresh df e cache
                        st.cache_data.clear()
                        st.session_state["df"] = load_df(json.loads(creds_json.to_json()), SOURCE_URL)
                        df = st.session_state["df"]
                        refresh_unique_aziende_cache()
                        st.session_state["data_version"] += 1
                        st.session_state["open_mass_rename"] = False

                        st.success(f"✅ Rinomina completata: {changed} occorrenze aggiornate.")
                        st.toast("Azienda rinominata globalmente", icon="✅")
                        st.rerun()
                    except Exception as e:
                        st.error("❌ Errore durante la rinomina massiva:")
                        st.exception(e)

        # =========================
        # Editor per gli altri campi (escludo 'Azienda' perché gestito sopra)
        # =========================
        other_cols = [c for c in WRITE_COLS if c != "Azienda"]
        pairs = [{"Campo": c, "Valore": to_clean_str(full_row.get(c, ""))} for c in other_cols]
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

        st.caption("Valore attuale di 'art_desart' (non modificabile, copiato in 'art_desart_precedente' al salvataggio):")
        st.code(to_clean_str(full_row.get("art_desart", "")))

        url_img = to_clean_str(full_row.get("URL_immagine", ""))
        if url_img:
            try:
                st.image(url_img, use_column_width=True, caption="Anteprima immagine")
            except Exception:
                pass

        st.success("Salvataggio: scrive **solo** le colonne specificate, direttamente nell'origine (gid=560544700).")

        if st.button("💾 Salva nell'origine"):
            try:
                # mappa valori da editor (solo WRITE_COLS tranne Azienda, che prendo dal selettore)
                values_map = {}
                for _, r in edited_detail.iterrows():
                    campo = to_clean_str(r.get("Campo", ""))
                    if campo and campo in other_cols:
                        values_map[campo] = to_clean_str(r.get("Valore", ""))

                # aggiungi Azienda dal selettore, normalizzando spazi
                values_map["Azienda"] = normalize_spaces(azienda_selected or new_azienda_input)

                # art_kart obbligatorio pulito
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

                # upsert SOLO sulle 9 colonne, art_desart_precedente = art_desart attuale
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

                # aggiorna cache aziende (idempotenza)
                st.session_state["df"] = df_local
                refresh_unique_aziende_cache()
                st.session_state["data_version"] += 1

                if result == "updated":
                    st.success("✅ Riga aggiornata. UI aggiornata subito.")
                elif result == "added":
                    st.success("✅ Nuova riga aggiunta. UI aggiornata subito.")

                st.toast("Salvato!", icon="✅")
                st.rerun()

            except Exception as e:
                st.error("❌ Errore durante il salvataggio:")
                st.exception(e)
