# app.py
import json, re
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

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
st.set_page_config(page_title="📚 Catalogo Articoli – AgGrid + Edit (Fast Refresh)", layout="wide")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly",
]
REDIRECT_URI = "http://localhost"

SOURCE_URL = st.secrets["sheet"]["url"]  # gid=560544700 (listing)
DEST_URL = "https://docs.google.com/spreadsheets/d/1_mwlW5sklv-D_992aWC--S3nfg-OJNOs4Nn2RZr8IPE/edit?gid=405669789#gid=405669789"

# -------------------------------------------------
# UTILS
# -------------------------------------------------
def parse_sheet_url(url: str):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m: raise ValueError("URL Google Sheet non valido.")
    spreadsheet_id = m.group(1)
    parsed = urlparse(url); gid = None
    if parsed.query:
        q = parse_qs(parsed.query)
        gid = (q.get("gid") or [None])[0]
    if (not gid) and parsed.fragment and parsed.fragment.startswith("gid="):
        gid = parsed.fragment.split("gid=")[1]
    return spreadsheet_id, (gid or "0")

def build_flow() -> Flow:
    oc = st.secrets["oauth_client"]
    client_conf = {"installed":{
        "client_id": oc["client_id"],
        "project_id": oc.get("project_id",""),
        "auth_uri": oc["auth_uri"],
        "token_uri": oc["token_uri"],
        "auth_provider_x509_cert_url": oc["auth_provider_x509_cert_url"],
        "client_secret": oc["client_secret"],
        "redirect_uris": oc.get("redirect_uris", [REDIRECT_URI]),
    }}
    return Flow.from_client_config(client_conf, scopes=SCOPES, redirect_uri=REDIRECT_URI)

def get_creds():
    if st.sidebar.button("🔁 Reset login Google"):
        st.session_state.pop("oauth_token", None)
        st.session_state.pop("db_df", None)   # ★ reset cache locale
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
    st.sidebar.info("1) Autorizza → 2) Copia l’URL di http://localhost con code=… → 3) Incolla qui e Connetti")
    st.sidebar.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)
    pasted = st.sidebar.text_input("URL completo http://localhost… **o** solo il codice")
    if st.sidebar.button("✅ Connetti"):
        try:
            raw = pasted.strip()
            if raw.startswith("http"):
                parsed = urlparse(raw); code = (parse_qs(parsed.query).get("code") or [None])[0]
            else:
                code = raw
            if not code:
                st.sidebar.error("Manca `code`.")
                return None
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.sidebar.success("Autenticazione completata ✅")
            return creds
        except Exception as e:
            msg = str(e)
            if "scope has changed" in msg.lower():
                st.sidebar.warning("Scope cambiati: reset login…")
                st.session_state.pop("oauth_token", None); st.cache_data.clear(); st.rerun()
            st.sidebar.error(f"Errore OAuth: {e}")
            return None
    return None

def get_gc(creds_json: dict) -> gspread.Client:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    return gspread.authorize(creds)

@st.cache_data(ttl=300, show_spinner=True)
def load_df_from_source(creds_json: dict, sheet_url: str) -> pd.DataFrame:
    gc = get_gc(creds_json)
    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None: raise RuntimeError(f"Nessun worksheet con gid={gid}.")
    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0)
    if df is None: df = pd.DataFrame()
    elif not isinstance(df, pd.DataFrame): df = pd.DataFrame(df)
    df = df.dropna(how="all")
    for c in ["art_kart","art_desart","art_kmacro","DescrizioneAffinata","URL_immagine"]:
        if c not in df.columns: df[c] = pd.NA
        df[c] = df[c].astype("string").fillna("")
    return df

def load_target_ws(gc: gspread.Client, dest_url: str):
    spreadsheet_id, gid = parse_sheet_url(dest_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None: raise RuntimeError(f"Nessun worksheet DEST con gid={gid}.")
    return ws

def get_ws_header(ws: gspread.Worksheet):
    return ws.row_values(1)

def df_from_ws(ws: gspread.Worksheet) -> pd.DataFrame:
    df = get_as_dataframe(ws, evaluate_formulas=False, include_index=False, header=0)
    if df is None: df = pd.DataFrame()
    elif not isinstance(df, pd.DataFrame): df = pd.DataFrame(df)
    df = df.dropna(how="all")
    for col in df.columns: df[col] = df[col].astype("string").fillna("")
    return df

def upsert_row_by_art_kart(ws: gspread.Worksheet, values_map: dict, key_col="art_kart"):
    header = get_ws_header(ws)
    if key_col not in header:
        raise RuntimeError(f"Colonna chiave '{key_col}' mancante nel foglio di destinazione.")
    row_vals = [("" if values_map.get(h) is None else str(values_map.get(h, ""))) for h in header]
    df_dest = df_from_ws(ws)
    if df_dest.empty:
        ws.append_row(row_vals, value_input_option="USER_ENTERED")
        return "added"
    exists = False; target_idx = None
    if key_col in df_dest.columns:
        matches = df_dest.index[df_dest[key_col] == str(values_map.get(key_col, ""))].tolist()
        if matches: exists = True; target_idx = matches[0]
    else:
        raise RuntimeError(f"Il foglio di destinazione non ha la colonna '{key_col}'.")
    if exists:
        confirm_key = "confirm_overwrite"
        if not st.session_state.get(confirm_key, False):
            st.warning("⚠️ Record con lo stesso 'art_kart' già presente. Confermi di sovrascrivere?")
            if st.button("Confermo sovrascrittura"): st.session_state[confirm_key] = True; st.experimental_rerun()
            return "await_confirm"
        row_number = 2 + target_idx
        start = rowcol_to_a1(row_number, 1); end = rowcol_to_a1(row_number, len(header))
        ws.update(f"{start}:{end}", [row_vals], value_input_option="USER_ENTERED")
        st.session_state.pop(confirm_key, None)
        return "updated"
    else:
        ws.append_row(row_vals, value_input_option="USER_ENTERED")
        return "added"

# -------------------------------------------------
# SIDEBAR: Auth + Filtri
# -------------------------------------------------
st.sidebar.header("🔐 Autenticazione Google")
creds = get_creds()
if not creds: st.stop()

# ★ Caricamento “una volta” + cache locale velocissima
if "db_df" not in st.session_state:
    try:
        df0 = load_df_from_source(json.loads(creds.to_json()), SOURCE_URL)
        st.session_state["db_df"] = df0  # ★ viva in RAM sessione
    except Exception as e:
        st.error("❌ Errore caricando il foglio sorgente."); st.exception(e); st.stop()
df = st.session_state["db_df"]  # ★ usiamo SEMPRE la copia locale

st.sidebar.header("🎛️ Filtri")
f_code = st.sidebar.text_input("art_kart (codice articolo)")
f_desc = st.sidebar.text_input("art_desart (descrizione Bollicine)")
reparti = sorted([v for v in df["art_kmacro"].dropna().unique() if str(v).strip()!=""])
f_reps = st.sidebar.multiselect("art_kmacro (reparto)", reparti)
pres = st.sidebar.radio("DescrizioneAffinata", ["Qualsiasi","Presente","Assente"], index=0)
f_aff = st.sidebar.text_input("Cerca in DescrizioneAffinata")

# Filtri rapidi
mask = pd.Series(True, index=df.index)
if f_code.strip(): mask &= df["art_kart"].str.contains(re.escape(f_code.strip()), case=False, na=False)
if f_desc.strip(): mask &= df["art_desart"].str.contains(re.escape(f_desc.strip()), case=False, na=False)
if f_reps:        mask &= df["art_kmacro"].isin(f_reps)
if pres=="Presente": mask &= df["DescrizioneAffinata"].str.strip()!=""
elif pres=="Assente": mask &= df["DescrizioneAffinata"].str.strip()==""
if f_aff.strip(): mask &= df["DescrizioneAffinata"].str.contains(re.escape(f_aff.strip()), case=False, na=False)

filtered = df.loc[mask].copy()

# -------------------------------------------------
# MAIN: sx Risultati (AgGrid), dx Dettaglio verticale
# -------------------------------------------------
left, right = st.columns([2,1], gap="large")

with left:
    st.subheader("📋 Risultati")
    result_cols = ["art_kart","art_desart","DescrizioneAffinata","URL_immagine"]
    present_cols = [c for c in result_cols if c in filtered.columns]
    filtered_results = filtered[present_cols].copy()

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
    )

    selected_rows = grid_resp.get("selected_rows", [])
    if isinstance(selected_rows, pd.DataFrame):
        selected_rows = selected_rows.to_dict(orient="records")
    elif isinstance(selected_rows, dict):
        selected_rows = [selected_rows]
    elif selected_rows is None:
        selected_rows = []
    elif not isinstance(selected_rows, list):
        try: selected_rows = list(selected_rows)
        except Exception: selected_rows = []
    selected_row = selected_rows[0] if len(selected_rows)>0 else None

with right:
    st.subheader("🔎 Dettaglio riga selezionata (editabile)")
    if selected_row is None:
        st.info("Seleziona una riga nella tabella a sinistra.")
    else:
        # prendi riga completa da db_df
        full_row = None
        if "art_kart" in selected_row and "art_kart" in df.columns:
            key = str(selected_row["art_kart"])
            matches = df[df["art_kart"] == key]
            if not matches.empty: full_row = matches.iloc[0]
        if full_row is None:
            full_row = pd.Series({c: selected_row.get(c, "") for c in df.columns})

        # Editor verticale (Campo, Valore)
        detail_pairs = [{"Campo": c, "Valore": str(full_row.get(c, ""))} for c in df.columns]
        detail_table = pd.DataFrame(detail_pairs, columns=["Campo","Valore"])
        edited_detail = st.data_editor(
            detail_table, use_container_width=True, hide_index=True, num_rows="dynamic",
            column_config={
                "Campo": st.column_config.TextColumn(help="Nome colonna nel foglio"),
                "Valore": st.column_config.TextColumn(help="Valore da salvare"),
            },
            key="detail_editor"
        )

        # Mini anteprima immagine
        try:
            url_img = str(full_row.get("URL_immagine","")).strip()
            if url_img: st.image(url_img, use_column_width=True, caption="Anteprima immagine")
        except Exception: pass

        st.success("Destinazione: stesso file, worksheet gid=405669789")
        if st.button("💾 Salva su foglio", use_container_width=True):
            try:
                # Ricostruisci mappa colonna→valore dal formato verticale
                values_map = {}
                for _, r in edited_detail.iterrows():
                    campo = str(r.get("Campo","")).strip()
                    if campo=="" or campo.lower()=="nan": continue
                    values_map[campo] = "" if pd.isna(r.get("Valore")) else str(r.get("Valore"))

                art_val = str(values_map.get("art_kart","")).strip()
                if not art_val:
                    st.error("Campo 'art_kart' obbligatorio."); st.stop()

                # Scrivi su DEST
                creds_json = json.loads(Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES).to_json())
                gc = get_gc(creds_json)
                ws_dest = load_target_ws(gc, DEST_URL)
                result = upsert_row_by_art_kart(ws_dest, values_map, key_col="art_kart")

                if result == "await_confirm":
                    st.warning("Conferma richiesta: premi 'Confermo sovrascrittura'.")
                    st.stop()

                # ★ FAST REFRESH: aggiorna SUBITO il database locale (db_df)
                db_df = st.session_state["db_df"].copy()
                if (db_df["art_kart"] == art_val).any():
                    # update in-place delle colonne presenti
                    idx = db_df.index[db_df["art_kart"] == art_val][0]
                    for k, v in values_map.items():
                        if k in db_df.columns:
                            db_df.at[idx, k] = v
                        else:
                            # se nuova colonna, aggiungila e valorizza solo per la riga
                            db_df[k] = db_df.get(k, pd.NA).astype("string")
                            db_df.at[idx, k] = v
                else:
                    # append nuova riga rispettando tutte le colonne attuali
                    new_row = {c: "" for c in db_df.columns}
                    for k, v in values_map.items():
                        if k in db_df.columns:
                            new_row[k] = v
                        else:
                            db_df[k] = db_df.get(k, pd.NA).astype("string")
                            new_row[k] = v
                    db_df = pd.concat([db_df, pd.DataFrame([new_row])], ignore_index=True)

                # normalizza i tipi string
                for col in db_df.columns:
                    db_df[col] = db_df[col].astype("string").fillna("")

                st.session_state["db_df"] = db_df  # ★ sostituisci cache locale
                # niente round-trip a Google → UI aggiornata al volo
                if result == "updated":
                    st.success("✅ Riga sovrascritta. (UI aggiornata)")
                else:
                    st.success("✅ Nuova riga aggiunta. (UI aggiornata)")

                # rerun per ridisegnare grid/filtri già con i nuovi valori
                st.experimental_rerun()

            except Exception as e:
                st.error("❌ Errore durante il salvataggio:")
                st.exception(e)
