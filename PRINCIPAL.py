# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""


import streamlit as st
from PIL import Image

#from pages.COMUNES import FUNCIONES_AUXILIARES

import TEMP #import ENTRADA_DATOS,CONSULTA_ESTADO,CONSULTA_ESTADILLOS

logo_IEO_principal = 'DATOS/IMAGENES/logo-CSIC.jpg'
logo_IEO_reducido  = 'DATOS/IMAGENES/ieo.ico'

imagen_logo   = Image.open(logo_IEO_reducido)
imagen_pagina = Image.open(logo_IEO_principal)

# Encabezados y titulos 
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 
st.title("Servicio de información de nutrientes del C.O de A Coruña")

# Añade el logo del IEO


st.image(imagen_pagina)

paginas = {"ENTRADA DATOS": TEMP.ENTRADA_DATOS,
    "CONSULTA ESTADO": TEMP.CONSULTA_ESTADO,
    "CONSULTA ESTADILLOS": TEMP.CONSULTA_ESTADILLOS
}

seleccion = st.sidebar.radio("Elige la página: ",tuple(paginas.keys()))

paginas[seleccion]()

# # Autentica al usuario
# if FUNCIONES_AUXILIARES.check_password():
#     st.write("USUARIO IDENTIFICADO CORRECTAMENTE")
#     ENTRADA_DATOS.createPage()
    # app.add_page('page1', page1.app)
    # app.add_page('page2', page2.app)

