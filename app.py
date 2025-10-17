import re
import io
import pandas as pd
import streamlit as st
import gspread
from gspread_dataframe import get_as_dataframe
from urllib.parse import urlparse, parse_qs

st.set_page_config(page_title="📊 Catalogo – Filtro Articoli", layout="wide")

# =========================
# Utility: parsing URL Sheet per ricavare spreadsheet id e gid
# =========================
def parse_sheet_url(sheet_url: str):
    """
    Accetta un URL tipo:
    https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid=560544700#gid=560544700
    Ritorna (spreadsheet_id, gid)
    """
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("URL del Google Sheet non valido.")
    spreadsheet_id = m.group(1)

    # Prova a leggere gid da query o da hash
    parsed = urlparse(sheet_url)
    gid = None
    # query
    if parsed.query:
        q = parse_qs(parsed.query)
        if "gid" in q and len(q["gid"]) > 0:
            gid = q["gid"][0]
    # hash
    if (not gid) and parsed.fragment:
        frag = parsed.fragment
        if frag.startswith("gid="):
            gid = frag.split("gid=")[1]

    # fallback
    if gid is None:
        gid = "0"

    return spreadsheet_id, gid

# =========================
# Caricamento Google Sheet
# =========================
@st.cache_data(ttl=300, show_spinner=True)
def load_dataframe(sheet_url: str) -> pd.DataFrame:
    """Carica il worksheet indicato da gid nel Google Sheet e lo restituisce come DataFrame."""
    # Credenziali Service Account da st.secrets
    sa_info = st.secrets["gcp_service_account"]

    client = gspread.service_account_from_dict(sa_info)

    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = client.open_by_key(spreadsheet_id)

    # Trova il worksheet con quel gid
    ws = None
    for w in sh.worksheets():
        # gspread espone w.id come int
        if str(w.id) == str(gid):
            ws = w
            break

    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid} trovato.")

    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0)
    # Pulisci completamente le righe vuote eventuali:
    if df is None:
        df = pd.DataFrame()
    df = df.dropna(how="all")
    # Normalizza colonne chiave se presenti
    rename_map = {
        # Se i nomi sono esattamente questi, ok; in caso contrario, adegua qui.
        "art_kart": "art_kart",
        "art_desart": "art_desart",
        "art_kmacro": "art_kmacro",
        "DescrizioneAffinata": "DescrizioneAffinata",
    }
    # Garantiamo che le colonne ci siano; se mancano, le creiamo vuote
    for col in rename_map.keys():
        if col not in df.columns:
            df[col] = pd.NA

    # Converte tutto a stringa per ricerche/filtri testuali più robuste
    for col in ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]:
        df[col] = df[col].astype("string").fillna("")

    return df

# =========================
# Sidebar – Sorgente dati
# =========================
st.sidebar.header("⚙️ Impostazioni")
sheet_url = st.secrets.get("sheet", {}).get("url", "")
if not sheet_url:
    # fallback: permetti inserimento manuale
    sheet_url = st.sidebar.text_input(
        "URL Google Sheet", 
        value="https://docs.google.com/spreadsheets/d/1_mwlW5sklv-D_992aWC--S3nfg-OJNOs4Nn2RZr8IPE/edit?gid=560544700#gid=560544700"
    )

with st.sidebar:
    st.caption("Assicurati che il foglio sia condiviso col Service Account in sola lettura.")

# =========================
# Carica dati
# =========================
try:
    df = load_dataframe(sheet_url)
except Exception as e:
    st.error(f"Errore nel caricamento del Google Sheet: {e}")
    st.stop()

st.title("📚 Catalogo Articoli – Filtri rapidi")

# =========================
# Filtri
# =========================
with st.expander("Filtri", expanded=True):
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        f_codice = st.text_input("art_kart (codice articolo):", placeholder="es. 12345 o parte del codice")
        f_desc_bollicine = st.text_input("art_desart (descrizione Bollicine):", placeholder="testo libero")

    with col2:
        # Multi-selezione reparto (art_kmacro)
        reparti = sorted([v for v in df["art_kmacro"].dropna().unique() if str(v).strip() != ""])
        f_reparti = st.multiselect("art_kmacro (reparto):", options=reparti, placeholder="Seleziona uno o più reparti")

    with col3:
        # Presenza DescrizioneAffinata: Qualsiasi / Presente / Assente
        f_affinata = st.radio(
            "DescrizioneAffinata:",
            options=["Qualsiasi", "Presente", "Assente"],
            index=0,
            help="Filtra se la descrizione affinata è presente o meno."
        )
        # Filtro testo su DescrizioneAffinata (opzionale)
        f_affinata_testo = st.text_input("Cerca in DescrizioneAffinata:", placeholder="testo libero")

# Applichiamo i filtri
mask = pd.Series(True, index=df.index)

# 1) art_kart (contains, case-insensitive)
if f_codice.strip():
    mask &= df["art_kart"].str.contains(re.escape(f_codice.strip()), case=False, na=False)

# 2) art_desart (contains)
if f_desc_bollicine.strip():
    mask &= df["art_desart"].str.contains(re.escape(f_desc_bollicine.strip()), case=False, na=False)

# 3) art_kmacro in selezione
if f_reparti:
    mask &= df["art_kmacro"].isin(f_reparti)

# 4) DescrizioneAffinata presenza/assenza
if f_affinata == "Presente":
    mask &= df["DescrizioneAffinata"].str.strip() != ""
elif f_affinata == "Assente":
    # vuota o solo spazi
    mask &= df["DescrizioneAffinata"].str.strip() == ""

# 5) Ricerca testo in DescrizioneAffinata
if f_affinata_testo.strip():
    mask &= df["DescrizioneAffinata"].str.contains(re.escape(f_affinata_testo.strip()), case=False, na=False)

filtered = df.loc[mask].copy()

# =========================
# Risultati
# =========================
st.markdown(f"**Risultati:** {len(filtered):,} righe")

# Mostra colonne principali in ordine utile
cols_show = ["art_kart", "art_desart", "art_kmacro", "DescrizioneAffinata"]
present_cols = [c for c in cols_show if c in filtered.columns]
st.dataframe(filtered[present_cols], use_container_width=True)

# =========================
# Download CSV
# =========================
def to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8")

csv_bytes = to_csv_bytes(filtered[present_cols] if present_cols else filtered)
st.download_button(
    label="⬇️ Scarica CSV filtrato",
    data=csv_bytes,
    file_name="articoli_filtrati.csv",
    mime="text/csv"
)

# =========================
# Hint di utilizzo
# =========================
with st.expander("ℹ️ Note"):
    st.write("""
- **Condivisione**: dai accesso *viewer* al Service Account sullo Sheet.
- **Prestazioni**: i dati sono **cache** per 5 minuti.
- **Ricerca parziale**: i filtri testuali fanno match *contiene* (case-insensitive).
- **Reparto**: puoi selezionare più reparti (multi-select).
- **DescrizioneAffinata**: scegli *Presente* o *Assente* oppure usa la ricerca testuale.
""")
