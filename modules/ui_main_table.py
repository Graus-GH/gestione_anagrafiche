import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

def show(df):
    st.subheader("📄 Risultati")
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=30)
    gb.configure_grid_options(rowHeight=28, headerHeight=32)
    grid_options = gb.build()

    grid_resp = AgGrid(
        df,
        gridOptions=grid_options,
        height=420,
        allow_unsafe_jscode=False,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="streamlit"
    )

    sel = grid_resp.get("selected_rows", [])
    if not sel:
        return None

    # recupera indice originale (AgGrid restituisce dict)
    selected_row = sel[0]
    selected_idx = selected_row.get("_st_index") or selected_row.get("_selectedRowNodeInfo", {}).get("nodeRowIndex")
    return selected_idx
