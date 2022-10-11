# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 13:09:09 2022

@author: ifraga
"""

import streamlit as st
import psycopg2
import pandas.io.sql as psql
import st_aggrid 

###############################################################################
###################### FUNCION CONEXIÓN #######################################
###############################################################################

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])



###############################################################################
####################### FUNCION LOG-IN ########################################
###############################################################################

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
        formulario = st.empty()
        with formulario.form('formulario'):
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
           formulario.empty()
           return True
        else:
           return False
       
    else:

        return True        
 
    
###############################################################################
####################### FUNCION ESTADO PROCESOS ########################################
###############################################################################
    
def estado_procesos():
    
    # Recupera los analisis disponibles
    conn = init_connection()
    df_muestreos = psql.read_sql('SELECT * FROM procesado_actual_nutrientes', conn)
    conn.close()
    
    # Seleccionar los muestreos en curso como aquellos con io_estado = 1
    df_muestreos_curso = df_muestreos[df_muestreos['io_estado']==1]

    # Elimina las columnas que no interesa mostrar
    df_muestreos_curso = df_muestreos_curso.drop(columns=['id_proceso','programa','io_estado','fecha_real_fin'])

    # Renombra las columnas
    df_muestreos_curso = df_muestreos_curso.rename(columns={'nombre_proceso':'Muestras','nombre_programa':'Programa','año':'Año','num_muestras':'Número muestras','fecha_inicio':'Inicio','fecha_estimada_fin':'Final estimado'})

    # Ajusta el formato de las fechas
    for idato in range(df_muestreos_curso.shape[0]):
        df_muestreos_curso['Inicio'][idato]         =  df_muestreos_curso['Inicio'][idato].strftime("%Y-%m-%d")
        df_muestreos_curso['Final estimado'][idato] =  df_muestreos_curso['Final estimado'][idato].strftime("%Y-%m-%d")
      
    # Muestra una tabla con los análisis en curso
    gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(df_muestreos_curso)
    gridOptions = gb.build()
    st_aggrid.AgGrid(df_muestreos_curso,gridOptions=gridOptions,height = 200,enable_enterprise_modules=True,allow_unsafe_jscode=True)    

    return df_muestreos_curso