"""
author: f.romadhana@gmail.com

"""

#import necessary libraries
import yaml
import pytz
import time
import pandas as pd
import streamlit as st
from time import sleep
from datetime import datetime
from pandas.api.types import (
                                is_categorical_dtype,
                                is_datetime64_any_dtype,
                                is_numeric_dtype,
                                is_object_dtype,
                            )
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy.engine import create_engine
from shillelagh.backends.apsw.db import connect


#set page configuration
st.set_page_config(
  page_title="Form Driver",
  page_icon="🚚",
  layout="wide")

#set padding page
st.markdown(f""" <style>
      .block-container{{
        padding-top: 1rem;
        padding-right: 1rem;
        padding-left: 1rem;
        padding-bottom: 2rem;
    }} </style> """, unsafe_allow_html=True)

#hide streamlit menu and footer
hide_menu_style = """
          <style>
          #MainMenu {visibility: hidden; }
          footer {visibility: hidden;}
          </style>
          """
st.markdown(hide_menu_style, unsafe_allow_html=True)
 
# ---USER AUTHENTICATION--- #
with open('user.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

name, authentication_status, username = authenticator.login('Login', 'main')

if st.session_state["authentication_status"] is False:
    st.error('Username & Password tidak terdaftar!')
if st.session_state["authentication_status"] is None:
    st.warning('Mohon isi Username & Password yang sudah diberikan!')

with open('user.yaml', 'w') as file:
    yaml.dump(config, file, default_flow_style=False)

#if login success, display form driver page
if st.session_state["authentication_status"]:
    #caching 
    @st.cache_data
    def process_for_index(index: int) -> int:
        sleep(0.5)
        return 2 * index + 1

    #sidebar
    authenticator.logout("Logout", "sidebar")
    st.sidebar.title(f"Hi, {name}!")

    #datetime now
    timenow = datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%d-%m-%Y %H:%M:%S")
    time.sleep(1)

    #filtering dataframe
    st.subheader("Tabel Konfirmasi Penerimaan Stock 🚚")
    def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        modify = st.checkbox("Gunakan Filter")
        if not modify:
            return df

        df = df.copy()

        #try to convert datetimes into a standard format (datetime, no timezone)
        for col in df.columns:
            if is_object_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col]).strftime("%d-%m-%Y")
                except Exception:
                    pass

            if is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(tz='Asia/Jakarta')

        modification_container = st.container()

        with modification_container:
            to_filter_columns = st.multiselect("Filter tabel telah aktif", df.columns)
            for column in to_filter_columns:
                left, right = st.columns((1, 20))
                left.write("↳")
                # Treat columns with < 10 unique values as categorical
                if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                    user_cat_input = right.multiselect(
                        f"Values for {column}",
                        df[column].unique(),
                        default=list(df[column].unique()),
                    )
                    df = df[df[column].isin(user_cat_input)]
                elif is_numeric_dtype(df[column]):
                    _min = float(df[column].min())
                    _max = float(df[column].max())
                    step = (_max - _min) / 100
                    user_num_input = right.slider(
                        f"Values for {column}",
                        _min,
                        _max,
                        (_min, _max),
                        step=step,
                    )
                    df = df[df[column].between(*user_num_input)]
                elif is_datetime64_any_dtype(df[column]):
                    user_date_input = right.date_input(
                        f"Values for {column}",
                        value=(
                            df[column].min(),
                            df[column].max(),
                        ),
                    )
                    if len(user_date_input) == 2:
                        user_date_input = tuple(map(pd.to_datetime, user_date_input))
                        start_date, end_date = user_date_input
                        df = df.loc[df[column].between(start_date, end_date)]
                else:
                    user_text_input = right.text_input(
                        f"Substring or regex in {column}",
                    )
                    if user_text_input:
                        df = df[df[column].str.contains(user_text_input)]

        return df

    #gcp credentials
    credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive.readonly', 
                'https://spreadsheets.google.com/feeds'])

    #shillelagh connection adapter
    connection = connect(":memory:", adapter_kwargs={
                "gsheetsapi" : { 
                "service_account_info": {
                    "type" : st.secrets["gcp_service_account"]["type"],
                    "project_id" : st.secrets["gcp_service_account"]["project_id"],
                    "private_key_id" : st.secrets["gcp_service_account"]["private_key_id"],
                    "private_key" : st.secrets["gcp_service_account"]["private_key"],
                    "client_email" : st.secrets["gcp_service_account"]["client_email"],
                    "client_id" : st.secrets["gcp_service_account"]["client_id"],
                    "auth_uri" : st.secrets["gcp_service_account"]["auth_uri"],
                    "token_uri" : st.secrets["gcp_service_account"]["token_uri"],
                    "auth_provider_x509_cert_url" : st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
                    "client_x509_cert_url" : st.secrets["gcp_service_account"]["client_x509_cert_url"]},
                    "subject" : st.secrets["gcp_service_account"]["client_email"],},},)

    #create a sqlalchemy engine using shillelagh as the backend
    engine = create_engine('shillelagh://', creator=lambda: connection)

    #read spreadsheet using sqlalchemy
    sheet_url = st.secrets["private_gsheets_url"]
    query = f'SELECT tanggal, nomor_po, nama_produk, order_supplier, konfirmasi_driver FROM "{sheet_url}"'
    result = connection.execute(query).fetchall()
    df = pd.DataFrame(result, columns=['tanggal', 'nomor_po', 'nama_produk', 'order_supplier', 'konfirmasi_driver'])

    st.write(":orange[Pilih tanggal & nomor po menggunakan filter]")
    edit_df = st.experimental_data_editor(filter_dataframe(df))

    #st.form to edit and save changes edited dataframe
    with st.form(key='edit_form'):
        #create a function to edit the dataframe
        def edit_dataframe(edit_df):
            edited_dataframe = edit_df.copy() #make a copy of the original dataframe
            #modify the dataframe here
            edited_dataframe['konfirmasi_driver'] = edited_dataframe['konfirmasi_driver']
            return edited_dataframe
        edited_df = edit_dataframe(edit_df)
        
        #add a button to submit the form
        submitted = st.form_submit_button(label='Simpan Data Baru', type="primary")
        if submitted:
            new_df = edited_df[['nomor_po', 'nama_produk', 'order_supplier', 'konfirmasi_driver']]
            st.dataframe(new_df)
            st.success("Konfirmasi driver telah tersimpan!")
            st.balloons()

            service = build('sheets', 'v4', credentials=credentials)
            def update_spreadsheet(new_df, spreadsheet_id, range_name):
                #convert the dataframe to a list of lists
                values = new_df.values.tolist()

                #update the values in the spreadsheet
                service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption='USER_ENTERED',
                    insertDataOption='INSERT_ROWS',
                    body={'values': values}
                ).execute()

            spreadsheet_id = st.secrets["spreadsheet_id"]
            range_name = st.secrets["range_name"]
            update_spreadsheet(new_df, spreadsheet_id, range_name)

        else:
            st.warning('Mohon cek fisik box dan konfirmasi jumlah stock yang diterima dari supplier', icon="⚠️")
            st.stop()
