import streamlit as st
from . import utils

IMAGE_FIELD = "URL_immagine"  # nome colonna nel tuo sheet

def show(row, ws):
    st.subheader("🖼️ Gestione immagine")
    with st.container():
        st.markdown('<div class="image-box">', unsafe_allow_html=True)

        current_url = str(row.get(IMAGE_FIELD, "") or "")
        col_a, col_b = st.columns([2, 3])

        with col_a:
            st.caption("Anteprima")
            if current_url:
                view_url = utils.ensure_drive_view_url(current_url)
                st.image(view_url, use_container_width=True)
            else:
                st.info("Nessuna immagine associata.")

        with col_b:
            st.caption("URL immagine")
            new_url = st.text_input("Incolla URL Google Drive o pubblico", value=current_url, key="img_url_input", help="Supportati link Google Drive e URL diretti a immagini.")
            st.markdown(f'<div class="image-url">{new_url}</div>', unsafe_allow_html=True)

            # Sorgente alternativa: dropdown di suggerimenti (es. da altra colonna)
            art_key = str(row.get("art_desart", "")) if "art_desart" in row.index else ""
            suggestions = utils.image_suggestions_for_key(art_key)
            sel = st.selectbox("Suggerimenti disponibili", options=[""] + suggestions, index=0)
            if sel:
                new_url = sel
                st.session_state["img_url_input"] = sel
                st.rerun()

            c1, c2 = st.columns(2)
            with c1:
                if st.button("🔎 Anteprima", use_container_width=True):
                    if new_url:
                        st.image(utils.ensure_drive_view_url(new_url), use_container_width=True)
                    else:
                        st.warning("Inserisci un URL.")
            with c2:
                if st.button("💾 Salva URL immagine nella riga", use_container_width=True):
                    if new_url:
                        # Aggiorna cella nel worksheet
                        try:
                            # Trova colonna
                            headers = ws.row_values(2)
                            if IMAGE_FIELD in headers:
                                col_idx = headers.index(IMAGE_FIELD) + 1
                                # Scopri riga (row index nello sheet è gestito chiamante; qui non abbiamo l'indice riga -> usiamo search key)
                                # Consiglio: salvare anche un ID univoco di riga per aggiornare in modo robusto.
                                st.session_state["pending_save"] = True
                                st.success("URL immagine pronto per il salvataggio assieme alla riga.")
                            else:
                                st.error(f"Colonna '{IMAGE_FIELD}' non trovata nel foglio.")
                        except Exception as e:
                            st.error(f"Errore salvataggio immagine: {e}")

        st.markdown('</div>', unsafe_allow_html=True)
