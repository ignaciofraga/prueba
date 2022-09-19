# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""


import streamlit as st
from PIL import Image

from pages.COMUNES import FUNCIONES_AUXILIARES

logo_IEO_principal = 'DATOS/IMAGENES/logo-CSIC.jpg'
logo_IEO_reducido  = 'DATOS/IMAGENES/ieo.ico'

imagen_logo   = Image.open(logo_IEO_reducido)
imagen_pagina = Image.open(logo_IEO_principal)

# Encabezados y titulos 
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 
st.title("Datos de nutrientes disponibles en el C.O de A Coruña")

# Añade el logo del IEO


st.image(imagen_pagina)

# Autentica al usuario
if FUNCIONES_AUXILIARES.check_password():
    st.write("Here goes your normal Streamlit app...")
    st.button("Click me")
