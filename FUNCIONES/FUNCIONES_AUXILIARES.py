# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 13:09:09 2022

@author: ifraga
"""

import streamlit as st
import psycopg2
import pandas.io.sql as psql

###############################################################################
###################### FUNCION CONEXIÓN #######################################
###############################################################################

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])



def log_in():
    

    # Si no está autenticado (no existe username), procede a identificar al usuario
    if 'usuario' not in st.session_state:
        # Recupera las contraseñas y usuarios de la base de datos
        conn        = init_connection()
        df_usuarios = psql.read_sql('SELECT * FROM usuarios_app', conn)
        conn.close()   
        
        listado_usuarios    = df_usuarios['nombre_usuario']
        listado_contrasenas = df_usuarios['password']
    
        # Despliega un formulario para introducir el nombre de usuario y la contraseña
        with st.form('formulario'):
            st.write("Introduzca sus datos de usuario y contraseña para acceder al servicio")
            col1, col2 = st.columns(2,gap="small")  
            with col1:
                usuario = st.selectbox('Selecciona el usuario',(listado_usuarios))
            with col2:
                contrasena = st.text_input("Contraseña", type="password")

            # Botón de envío para confirmar selección
            st.form_submit_button("Enviar")  
                   
        # comprueba si la contraseña introducida corresponde al usuario seleccionado    
        io_autorizado = 0 # por defecto no autorizado
        for iusuario_bd in range(len(listado_usuarios)):
            if usuario == listado_usuarios[iusuario_bd] and contrasena == listado_contrasenas[iusuario_bd]:
                io_autorizado = 1 # Autorizado!
                
        # Si el usuario está autorizado, devuelve "true" y añade al estado de la sesión el nombre de usuario
        if io_autorizado == 1:
           st.session_state['usuario'] = usuario
           del formulario
           return True
        else:
           return False
       
    else:

        return True        
    
   
    
    
    # # Devuelve "True" si la constraseña es correcta 

    # def password_entered(listado_usuarios,listado_contrasenas,usuario,contrasena):
        
    #     io_autorizado = 0
    #     for iusuario_bd in range(len(listado_usuarios)):
    #         if usuario == listado_usuarios[iusuario_bd] and contrasena == listado_contrasenas[iusuario_bd]:
    #             io_autorizado = 1
                
    #     if io_autorizado == 1:
    #        st.session_state["password_correct"] = True
    #        st.session_state["password"] = contrasena
    #        st.session_state["username"] = usuario
    #     else:
    #         st.session_state["password_correct"] = False

    # if "password_correct" not in st.session_state:
        
    #     # First run, show inputs for username + password.
    #     st.write('Introduzca sus datos de usuario y contraseña para acceder al servicio')
    #     col1, col2 = st.columns(2,gap="small")
    #     with col1:
    #         usuario = st.selectbox('Selecciona el usuario',(listado_usuarios),on_change=password_entered, key="username")
    #     #st.text_input("Usuario", on_change=password_entered, key="username")
    #     with col2:
    #         contrasena = st.text_input("Contraseña", type="password", on_change=password_entered, key="password")

    #     return False
    # elif not st.session_state["password_correct"]:
    #     # Password not correct, show input + error.
    #     st.write('Introduzca sus datos de usuario y contraseña para acceder al servicio')
    #     col1, col2 = st.columns(2,gap="small")
    #     with col1:
    #         usuario = st.selectbox('Selecciona el usuario',(listado_usuarios),on_change=password_entered, key="username")
    #     #st.text_input("Usuario", on_change=password_entered, key="username")
    #     with col2:
    #         contrasena = st.text_input("Contraseña", type="password", on_change=password_entered, key="password")
    #     st.error("Usuario no reconocido o contraseña incorrecta.")
    #     return False
    # else:
    #     # Password correct.
    #     return True
    
    
    
    
    

# def check_password(listado_usuarios):
#     """Returns `True` if the user had a correct password."""

#     def password_entered():
#         """Checks whether a password entered by the user is correct."""
#         if (
#             st.session_state["username"] in st.secrets["passwords"]
#             and st.session_state["password"]
#             == st.secrets["passwords"][st.session_state["username"]]
#         ):
#             st.session_state["password_correct"] = True
# #            del st.session_state["password"]  # don't store username + password
# #            del st.session_state["username"]
#         else:
#             st.session_state["password_correct"] = False

#     if "password_correct" not in st.session_state:
        
#         # First run, show inputs for username + password.
#         st.write('Introduzca sus datos de usuario y contraseña para acceder al servicio')
#         col1, col2 = st.columns(2,gap="small")
#         with col1:
#             st.selectbox('Selecciona el usuario',(listado_usuarios),on_change=password_entered, key="username")
#         #st.text_input("Usuario", on_change=password_entered, key="username")
#         with col2:
#             st.text_input("Contraseña", type="password", on_change=password_entered, key="password")

#         return False
#     elif not st.session_state["password_correct"]:
#         # Password not correct, show input + error.
#         st.write('Introduzca sus datos de usuario y contraseña para acceder al servicio')
#         col1, col2 = st.columns(2,gap="small")
#         with col1:
#             st.selectbox('Selecciona el usuario',(listado_usuarios),on_change=password_entered, key="username")
#         #st.text_input("Usuario", on_change=password_entered, key="username")
#         with col2:
#             st.text_input("Contraseña", type="password", on_change=password_entered, key="password")
#         st.error("Usuario no reconocido o contraseña incorrecta.")
#         return False
#     else:
#         # Password correct.
#         return True