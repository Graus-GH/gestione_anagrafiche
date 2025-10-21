# app.py – entry Streamlit, orchestra i moduli
import streamlit as st
from modules import auth, gsheet_utils, ui_main_table, ui_detail_panel, ui_image_manager, state_manager

# =========================
# CONFIG & STYLES
# =========================
st.set_page_config(page_title="📚 Catalogo Articoli – Edit in-place", layout="wide")
with open("assets/style.css", "r", encoding="utf-8") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

state_manager.init_session_defaults()

st.sidebar.title("⚙️ Azioni")
st.sidebar.button("🔄 Aggiorna Database", on_click=gsheet_utils.invalidate_cache)

# =========================
# LOGIN GOOGLE
# =========================
creds = auth.google_auth()
if not creds:
    st.stop()

# =========================
# DATI
# =========================
ws = gsheet_utils.open_sheet(creds)
df = gsheet_utils.load_data(ws)

# =========================
# UI: TABELLA + DETTAGLIO
# =========================
selected_idx = ui_main_table.show(df)
if selected_idx is None:
    st.info("⬆️ Seleziona una riga dalla tabella per vedere il dettaglio.")
    st.stop()

row = df.iloc[selected_idx].copy()
with st.container():
    updated_row, changed = ui_detail_panel.show(row, ws, df)

# =========================
# UI: GESTIONE IMMAGINI
# =========================
ui_image_manager.show(updated_row, ws)

# =========================
# SALVATAGGI
# =========================
if changed and st.session_state.get("pending_save", False):
    gsheet_utils.save_row(ws, df, selected_idx, updated_row)
    st.session_state["pending_save"] = False
    st.success("✅ Riga salvata.")
