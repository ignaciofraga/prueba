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
logo_IEO_reducido     = 'DATOS/IMAGENES/ieo.ico'

# Encabezado  
imagen_logo   = Image.open(logo_IEO_reducido)
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 

# Si el usuario está autorizado, despliega las webs a las que tiene acceso
# if FUNCIONES_AUXILIARES.check_password() is True:
if FUNCIONES_AUXILIARES.log_in() is True:
    
    # claúsula para manetener el nombre de usuario y poder identificar qué webs desplegar
    if 'usuario' in st.session_state:
    #     st.session_state['usuario'] = st.session_state["username"]

        if st.session_state["usuario"] == 'Usuario interno IEO':
        
            paginas = {"INICIO": PAGINAS.principal,
                        "ENTRADA DATOS NUTRIENTES": PAGINAS.entrada_datos,
                        "CONSULTA ESTADO PROCESADO": PAGINAS.consulta_estado,
                        "CONSULTA ESTADILLOS": PAGINAS.consulta_estadillos,
                        "CONSULTA EVOLUCION": PAGINAS.evolucion_analisis,
                        "ENTRADA PROCESOS": PAGINAS.entrada_procesos_actuales
                        }
            
        if st.session_state["usuario"] == 'Usuario externo':
            
            paginas = {"INICIO": PAGINAS.principal,
                        "ENTRADA ESTADILLOS": PAGINAS.entrada_estadillos}
        
        seleccion = st.sidebar.selectbox("Elige la página a mostrar: ",tuple(paginas.keys()))
        
        paginas[seleccion]()

# # Identificación y acceso
# io_acceso, usuario = FUNCIONES_AUXILIARES.check_password()


# # Si el usuario está autorizado, despliega las webs a las que tiene acceso
# if io_acceso is True:
    
#     if usuario == '1':
    
#         paginas = {"PRINCIPAL": PAGINAS.principal,
#                     "ENTRADA DATOS": PAGINAS.entrada_datos,
#                     "CONSULTA ESTADO": PAGINAS.consulta_estado}
 
#     if usuario == '2':
    
#         paginas = {"PRINCIPAL": PAGINAS.principal,
#                     "ENTRADA DATOS": PAGINAS.entrada_datos}
               
#     seleccion = st.sidebar.selectbox("Elige la página: ",tuple(paginas.keys()))
    
#     paginas[seleccion]()



