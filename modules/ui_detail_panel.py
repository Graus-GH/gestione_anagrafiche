import streamlit as st
import pandas as pd
from . import utils, state_manager

# Campi con logica di rinomina globale (puoi estendere)
RENAME_FIELDS = ["Azienda", "Prodotto", "gradazione", "annata", "Packaging", "Note"]

def _field_row(label, value, options=None, key=None, help_text=None):
    # layout in 3 colonne: label | input | help
    st.markdown('<div class="detail-row">', unsafe_allow_html=True)
    st.markdown(f'<div class="detail-label">{label}</div>', unsafe_allow_html=True)
    with st.container():
        if options is not None:
            new_val = st.selectbox("", options, index=utils.index_of(options, value), key=key, label_visibility="collapsed")
        else:
            new_val = st.text_input("", value or "", key=key)
    st.markdown(f'<div class="detail-help">{help_text or ""}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    return new_val

def show(row: pd.Series, ws, df: pd.DataFrame):
    st.subheader("🔎 Dettaglio riga")
    changed = False

    # Costruisci liste uniche per select
    uniques = {col: sorted(set(df[col].astype(str).fillna("").tolist())) for col in df.columns if col in RENAME_FIELDS}

    new_values = row.copy()

    for col in df.columns:
        lab = col
        val = row.get(col, "")
        key = f"detail_{col}"

        if col in uniques:
            options = [""] + [v for v in uniques[col] if v != ""]
            new_val = _field_row(lab, str(val), options=options, key=key, help_text="Rinomina globale disponibile")
        else:
            new_val = _field_row(lab, str(val), options=None, key=key, help_text="")

        if new_val != str(val):
            changed = True
            new_values[col] = new_val

    col1, col2, col3 = st.columns([1,1,2])
    with col1:
        if st.button("💾 Salva riga", use_container_width=True):
            st.session_state["pending_save"] = True
    with col2:
        # Rinomina globale: sostituisce TUTTE le occorrenze del valore vecchio con il nuovo per i campi ammessi
        if st.button("✏️ Rinomina globale (campi selezionati)", use_container_width=True):
            _global_rename(df, row, new_values)
            st.session_state["pending_save"] = True
            changed = True
    with col3:
        st.caption("Le modifiche saranno applicate e salvate nel foglio.")

    return new_values, changed

def _global_rename(df, old_row, new_row):
    for field in RENAME_FIELDS:
        old_val = str(old_row.get(field, ""))
        new_val = str(new_row.get(field, ""))
        if new_val and new_val != old_val:
            mask = df[field].astype(str) == old_val
            df.loc[mask, field] = new_val
