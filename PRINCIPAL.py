# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""

# Importa las funciones a utilizar
import streamlit as st
from PIL import Image

from FUNCIONES import FUNCIONES_AUXILIARES 
from FUNCIONES import PAGINAS

# Datos comunes utilizados por la aplicación
logo_IEO_principal    = 'DATOS/IMAGENES/logo-CSIC.jpg'
logo_IEO_reducido     = 'DATOS/IMAGENES/ieo.ico'
archivo_plantilla     = 'DATOS/PLANTILLA.xlsx'
archivo_instrucciones = 'DATOS/PLANTILLA.zip'


# Encabezado  
imagen_logo   = Image.open(logo_IEO_reducido)
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 

# Identificación y acceso
io_acceso, usuario = FUNCIONES_AUXILIARES.check_password()


# Si el usuario está autorizado, despliega las webs a las que tiene acceso
if io_acceso is True:
    
    if usuario == 'usuario_interno':
    
        paginas = {"PRINCIPAL": PAGINAS.principal(logo_IEO_principal),
                   "ENTRADA DATOS": PAGINAS.entrada_datos(archivo_plantilla,archivo_instrucciones),
                   "CONSULTA ESTADO": PAGINAS.consulta_estado}
 
    if usuario == 'usuario_externo':
    
        paginas = {"PRINCIPAL": PAGINAS.principal,
                   "ENTRADA DATOS": PAGINAS.entrada_datos(archivo_plantilla,archivo_instrucciones)}
               
    seleccion = st.sidebar.selectbox("Elige la página: ",tuple(paginas.keys()))
    
    paginas[seleccion]()



