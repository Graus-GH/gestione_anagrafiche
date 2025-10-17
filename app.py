import re, json, pandas as pd, streamlit as st, gspread
from gspread_dataframe import get_as_dataframe
from urllib.parse import urlparse, parse_qs
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request

st.set_page_config(page_title="📚 Catalogo Articoli – OAuth", layout="wide")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def parse_sheet_url(url:str):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m: raise ValueError("URL Google Sheet non valido.")
    spreadsheet_id = m.group(1)
    parsed = urlparse(url); gid = None
    if parsed.query:
        q = parse_qs(parsed.query)
        if "gid" in q and q["gid"]: gid = q["gid"][0]
    if (not gid) and parsed.fragment.startswith("gid="):
        gid = parsed.fragment.split("gid=")[1]
    return spreadsheet_id, (gid or "0")

def build_flow():
    oc = st.secrets["oauth_client"]
    client_conf = {"installed":{
        "client_id": oc["client_id"],
        "project_id": oc.get("project_id",""),
        "auth_uri": oc["auth_uri"],
        "token_uri": oc["token_uri"],
        "auth_provider_x509_cert_url": oc["auth_provider_x509_cert_url"],
        "client_secret": oc["client_secret"],
        "redirect_uris": oc.get("redirect_uris", ["http://localhost"]),
    }}
    return Flow.from_client_config(client_conf, scopes=SCOPES)

def get_creds():
    # token già in sessione?
    if "oauth_token" in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                st.session_state["oauth_token"] = json.loads(creds.to_json())
            except Exception as e:
                st.warning(f"Refresh token fallito, rifai login: {e}")
                st.session_state.pop("oauth_token", None)
                return None
        return creds

    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    st.info("1) Clicca per aprire Google, consenti l’accesso e copia il **codice** che ti mostra.")
    st.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)

    code = st.text_input("2) Incolla qui il **codice di verifica** di Google")
    if st.button("✅ Connetti"):
        try:
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.success("Autenticazione completata!")
            return creds
        except Exception as e:
            st.error(f"OAuth error: {e}")
    return None

@st.cache_data(ttl=300, show_spinner=True)
def load_df(creds_json: dict, sheet_url: str) -> pd.DataFrame:
    creds = Credentials.from_authorized_user_info(creds_json, SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet_id, gid = parse_sheet_url(sheet_url)
    sh = gc.open_by_key(spreadsheet_id)
    ws = next((w for w in sh.worksheets() if str(w.id) == str(gid)), None)
    if ws is None:
        raise RuntimeError(f"Nessun worksheet con gid={gid}")
    df = get_as_dataframe(ws, evaluate_formulas=True, include_index=False, header=0) or pd.DataFrame()
    df = df.dropna(how="all")
    # colonne chiave
    for c in ["art_kart","art_desart","art_kmacro","DescrizioneAffinata"]:
        if c not in df.columns: df[c] = pd.NA
        df[c] = df[c].astype("string").fillna("")
    return df

st.title("📚 Catalogo Articoli – Google OAuth")
sheet_url = st.secrets["sheet"]["url"]

creds = get_creds()
if not creds: st.stop()

try:
    df = load_df(json.loads(creds.to_json()), sheet_url)
except Exception as e:
    st.error(f"Errore caricando il foglio: {e}")
    st.stop()

with st.expander("Filtri", expanded=True):
    c1,c2,c3 = st.columns(3)
    with c1:
        f_code = st.text_input("art_kart")
        f_desc = st.text_input("art_desart")
    with c2:
        reps = sorted([v for v in df["art_kmacro"].dropna().unique() if str(v).strip()!=""])
        f_reps = st.multiselect("art_kmacro", reps)
    with c3:
        pres = st.radio("DescrizioneAffinata", ["Qualsiasi","Presente","Assente"], index=0)
        f_aff = st.text_input("Cerca in DescrizioneAffinata")

mask = pd.Series(True, index=df.index)
import re as _re
if f_code.strip(): mask &= df["art_kart"].str.contains(_re.escape(f_code.strip()), case=False)
if f_desc.strip(): mask &= df["art_desart"].str.contains(_re.escape(f_desc.strip()), case=False)
if f_reps: mask &= df["art_kmacro"].isin(f_reps)
if pres=="Presente": mask &= df["DescrizioneAffinata"].str.strip()!=""
elif pres=="Assente": mask &= df["DescrizioneAffinata"].str.strip()==""
if f_aff.strip(): mask &= df["DescrizioneAffinata"].str.contains(_re.escape(f_aff.strip()), case=False)

out = df.loc[mask, ["art_kart","art_desart","art_kmacro","DescrizioneAffinata"]]
st.markdown(f"**Risultati:** {len(out):,}")
st.dataframe(out, use_container_width=True)
st.download_button("⬇️ CSV", out.to_csv(index=False).encode(), "articoli_filtrati.csv", "text/csv")
