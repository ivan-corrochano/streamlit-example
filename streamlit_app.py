'''
Código para la creación de una aplicación para la realización de estudios
operacionales en streamlit
'''

import streamlit as st
import s3fs
import os
import pandas as pd

# Read secrets
AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']

# Create connection object
fs = s3fs.S3FileSystem(anon=False)

# Read luces.csv
luces = pd.read_csv(
    "s3://proyectos-internos/Estudios_Operacionales/Luces.csv",
    sep=';',
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY
    }
)

# An airport is selected
aeropuertos = luces['AEROPUERTO'].drop_duplicates()
aeropuerto = st.selectbox('Seleccione un aeropuerto', aeropuertos)

st.write('Has seleccionado', aeropuerto)
