from urllib.parse import urlparse, parse_qs
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json, streamlit as st

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
REDIRECT_URI = "http://localhost"

def build_flow():
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
    if "oauth_token" in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state["oauth_token"], SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            st.session_state["oauth_token"] = json.loads(creds.to_json())
        return creds

    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )

    st.info("1️⃣ Clicca il bottone qui sotto per aprire Google e consenti l’accesso.\n\n"
            "2️⃣ Ti comparirà un errore 'localhost non raggiungibile' → va bene.\n\n"
            "3️⃣ Copia **tutto l’URL** dalla barra del browser (inizia con `http://localhost/?code=`) e incollalo qui sotto.")
    st.link_button("🔐 Apri pagina di autorizzazione Google", auth_url)

    pasted = st.text_input("Incolla qui l’**URL completo** dopo il consenso")
    if st.button("✅ Connetti"):
        try:
            parsed = urlparse(pasted.strip())
            code = (parse_qs(parsed.query).get("code") or [None])[0]
            if not code:
                st.error("⚠️ URL non valido: non trovo `code=`. Assicurati di incollare l’URL intero da http://localhost/…")
                return None
            flow.fetch_token(code=code)
            creds = flow.credentials
            st.session_state["oauth_token"] = json.loads(creds.to_json())
            st.success("Autenticazione completata ✅")
            return creds
        except Exception as e:
            st.error(f"Errore OAuth: {e}")
            return None
    return None
