import streamlit as st
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe

@st.cache_data(show_spinner=False)
def open_sheet(creds):
    gc = gspread.authorize(creds)
    url = st.secrets["sheet"]["url"]              # <- imposta in secrets
    ws_name = st.secrets["sheet"].get("worksheet", "Catalogo")
    sh = gc.open_by_url(url)
    return sh.worksheet(ws_name)

@st.cache_data(show_spinner=True)
def load_data(ws):
    df = get_as_dataframe(ws, evaluate_formulas=True, header=1).fillna("")
    return df

def invalidate_cache():
    load_data.clear()
    open_sheet.clear()

def save_row(ws, df, idx, updated_row):
    """
    Scrive UNA riga nel worksheet usando l’indice DataFrame.
    Assumiamo header in riga 2 (header=1), dati reali dall’index 0 => riga sheet = idx + 3
    """
    writeable_df = df.copy()
    for col in updated_row.index:
        writeable_df.at[idx, col] = updated_row[col]

    # Aggiorna solo la riga sullo sheet
    rownum = idx + 3
    values = [writeable_df.columns.tolist()]  # header (per sicurezza)
    # Prepara valori riga (tutte le colonne)
    row_vals = [str(writeable_df.at[idx, c]) if pd.notna(writeable_df.at[idx, c]) else "" for c in writeable_df.columns]
    # Scrivi solo la riga (più efficiente: update(row, col, values))
    ws.update(f"A{rownum}:{chr(64+len(writeable_df.columns))}{rownum}", [row_vals])
