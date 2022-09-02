# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import psycopg2
import streamlit as st



# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])




### Consulta a la base de datos las fechas de los distintos procesos

nombre_programa = 'PELACUS'

conn = init_connection()
cursor = conn.cursor()

# Identificador del programa (PELACUS en este caso)
instruccion_sql = "SELECT id_programa FROM programas WHERE nombre_programa = '" + nombre_programa + "' ;"
cursor.execute(instruccion_sql)
id_programa =cursor.fetchall()
conn.commit()
cursor.close()
conn.close()

print(id_programa)

st.text(id_programa[0])
