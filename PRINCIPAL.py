# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""


import streamlit as st
from PIL import Image


logo_IEO_principal = 'DATOS/IMAGENES/logo-CSIC.jpg'
logo_IEO_reducido  = 'DATOS/IMAGENES/IEO.png'

# Encabezados y titulos 
st.set_page_config(page_title="Datos de nutrientes", layout="wide",page_icon=logo_IEO_reducido) 
st.title("Datos de nutrientes disponibles en el C.O de A Coruña")

# Añade el logo del IEO

imagen   = Image.open(logo_IEO_principal)
st.image(imagen)


