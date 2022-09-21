# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 13:09:09 2022

@author: ifraga
"""

import streamlit as st
import psycopg2


###############################################################################
###################### FUNCION CONEXIÓN #######################################
###############################################################################

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])




# def check_password():
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
#         st.text_input("Usuario", on_change=password_entered, key="username")
#         st.text_input(
#             "Contraseña", type="password", on_change=password_entered, key="password"
#         )

#         return False
#     elif not st.session_state["password_correct"]:
#         # Password not correct, show input + error.
#         st.text_input("Usuario", on_change=password_entered, key="username")
#         st.text_input(
#             "Contraseña", type="password", on_change=password_entered, key="password"
#         )
#         st.error("Usuario no reconocido o contraseña incorrecta.")
#         return False
#     else:
#         # Password correct.
#         return True



def check_password(listado_usuarios):
    """Returns `True` if the user had a correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if (
            st.session_state["username"] in st.secrets["passwords"]
            and st.session_state["password"]
            == st.secrets["passwords"][st.session_state["username"]]
        ):
            st.session_state["password_correct"] = True
#            del st.session_state["password"]  # don't store username + password
#            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        
        # First run, show inputs for username + password.
        st.write('Introduzca sus datos de usuario y contraseña para acceder al servicio')
        st.selectbox('Selecciona el usuario',(listado_usuarios),on_change=password_entered, key="username")
        #st.text_input("Usuario", on_change=password_entered, key="username")
        st.text_input(
            "Contraseña", type="password", on_change=password_entered, key="password"
        )

        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.selectbox('Selecciona el usuario',(listado_usuarios),on_change=password_entered, key="username")
        #st.text_input("Usuario", on_change=password_entered, key="username")
        st.text_input(
            "Contraseña", type="password", on_change=password_entered, key="password"
        )
        st.error("Usuario no reconocido o contraseña incorrecta.")
        return False
    else:
        # Password correct.
        return True