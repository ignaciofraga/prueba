# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""


import streamlit as st
from PIL import Image

#from pages.COMUNES import FUNCIONES_AUXILIARES

import AUXILIAR #import ENTRADA_DATOS,CONSULTA_ESTADO,CONSULTA_ESTADILLOS

logo_IEO_principal = 'DATOS/IMAGENES/logo-CSIC.jpg'
logo_IEO_reducido  = 'DATOS/IMAGENES/ieo.ico'

imagen_logo   = Image.open(logo_IEO_reducido)
imagen_pagina = Image.open(logo_IEO_principal)

# Encabezados y titulos 
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 

def principal():

    st.title("Servicio de información de nutrientes del C.O de A Coruña")

    # Añade el logo del IEO
    st.image(imagen_pagina)


def password_entered():
    """Checks whether a password entered by the user is correct."""
    if (
        st.session_state["username"] in st.secrets["passwords"]
        and st.session_state["password"]
        == st.secrets["passwords"][st.session_state["username"]]
    ):
        st.session_state["password_correct"] = True
        del st.session_state["password"]  # don't store username + password
        del st.session_state["username"]
    else:
        st.session_state["password_correct"] = False



st.text('Introduzca sus datos de usuario y contraseña para acceder al servicio')
username = st.text_input("Usuario", on_change=password_entered, key="username")
st.text_input(
    "Contraseña", type="password", on_change=password_entered, key="password"
)

st.text(username)

# if AUXILIAR.check_password():
#     #st.write("USUARIO IDENTIFICADO CORRECTAMENTE")
    
    
#     paginas = {"PRINCIPAL": principal,
#         "ENTRADA DATOS": AUXILIAR.entrada_datos,
#         "CONSULTA ESTADO": AUXILIAR.consulta_estado}
#     #    "CONSULTA ESTADILLOS": TEMP.CONSULTA_ESTADILLOS
#     #}
    
#     seleccion = st.sidebar.selectbox("Elige la página: ",tuple(paginas.keys()))
    
#     paginas[seleccion]()

# # Autentica al usuario
# if FUNCIONES_AUXILIARES.check_password():
#     st.write("USUARIO IDENTIFICADO CORRECTAMENTE")
#     ENTRADA_DATOS.createPage()
    # app.add_page('page1', page1.app)
    # app.add_page('page2', page2.app)

