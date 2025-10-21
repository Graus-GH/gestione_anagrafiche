import streamlit as st

def init_session_defaults():
    defaults = {
        "creds": None,
        "pending_save": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
