import streamlit as st
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive",
]

def google_auth():
    """
    Gestisce OAuth2. Richiede in .streamlit/secrets.toml:
    [google]
    client_id=""
    client_secret=""
    """
    if "creds" in st.session_state and st.session_state.creds and st.session_state.creds.valid:
        return st.session_state.creds

    client_id = st.secrets["google"]["client_id"]
    client_secret = st.secrets["google"]["client_secret"]
    client_config = {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = Flow.from_client_config(client_config, scopes=SCOPES)
    flow.redirect_uri = "http://localhost"
    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline", include_granted_scopes="true")

    st.markdown(f"[🔐 Autorizza l’accesso a Google]({auth_url})")
    code = st.text_input("Incolla qui il codice di autorizzazione Google:")
    if not code:
        return None

    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        st.session_state.creds = creds
        st.success("✅ Accesso effettuato.")
        return creds
    except Exception as e:
        st.error(f"Errore OAuth: {e}")
        return None
