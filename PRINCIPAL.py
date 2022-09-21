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
# logo_IEO_principal    = 'DATOS/IMAGENES/logo-CSIC.jpg'
logo_IEO_reducido     = 'DATOS/IMAGENES/ieo.ico'
# archivo_plantilla     = 'DATOS/PLANTILLA.xlsx'
# archivo_instrucciones = 'DATOS/PLANTILLA.zip'


# Encabezado  
imagen_logo   = Image.open(logo_IEO_reducido)
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 



# with st.form("Formulario autenticación"):

#     listado_usuarios = ['1','2']
 
#     col1, col2 = st.columns(2,gap="small")
#     with col1:
#         tipo_usuario_elegido = st.selectbox('Selecciona el tipo de usuario', (listado_usuarios))
#     with col2:
#         contrasena           = st.text_input("Contraseña", type="password")

#     # Botón de envío para confirmar selección
#     submitted = st.form_submit_button("Submit")











# Si el usuario está autorizado, despliega las webs a las que tiene acceso
if FUNCIONES_AUXILIARES.check_password() is True:
    
    if 'usuario' not in st.session_state:
        st.session_state['usuario'] = st.session_state["username"]

    #st.text(usuario)
    st.write(st.session_state.usuario)
    
    
    paginas = {"PRINCIPAL": PAGINAS.principal,
                "ENTRADA DATOS": PAGINAS.entrada_datos,
                "CONSULTA ESTADO": PAGINAS.consulta_estado}
    
    seleccion = st.sidebar.selectbox("Elige la página: ",tuple(paginas.keys()))
    
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



