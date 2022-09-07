# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import streamlit as st
import psycopg2


import pandas.io.sql as psql


import datetime

from pages.COMUNES import FUNCIONES_INSERCION

# DATOS BASE

logo_IEO_reducido            =  'DATOS/IMAGENES/ieo.ico'
archivo_plantilla            =  'DATOS/PLANTILLA.xlsx'
archivo_instrucciones        =  'DATOS/INSTRUCCIONES_PLANTILLA.zip'
archivo_variables_base_datos =  'DATOS/VARIABLES.xlsx'


##### FUNCIONES AUXILIARES ######

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])




##### WEB STREAMLIT #####

# Encabezados y titulos 
st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
st.title('Servicio de entrada de datos en la base de datos del C.O de A Coruña')

# Recupera la tabla de los programas disponibles en la base de datos, como un dataframe
conn = init_connection()
df_programas = psql.read_sql('SELECT * FROM programas', conn)
conn.close()

col1, col2 = st.columns(2)

with col1:
    # Formulario 
    # Listado del tipo de dato a introducir      
    listado_opciones = ['Análisis de laboratorio (nuevos datos)','Procesado o revisión de datos ya disponibles']
    
    # Selección de programa, tipo de dato y correo de contacto
    programa_elegido  = st.selectbox('Selecciona el programa al que corresponden los datos a insertar',(df_programas['nombre_programa']))
    tipo_dato_elegido = st.selectbox('Selecciona el origen de los datos a insertar', (listado_opciones))
    email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")

with col2:
    fecha_actualizacion = st.date_input("Selecciona fecha de procesado",datetime.date.today())


### Recupera los identificadores de la selección hecha

# Recupera el identificador del programa seleccionado
id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

# Encuentra el identificador asociado al tipo de dato
for iorigen in range(len(listado_opciones)):
    if listado_opciones[iorigen] == tipo_dato_elegido:
        id_opcion_elegida = iorigen
        
### Subida de archivos

# Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
direccion_host = st.secrets["postgres"].host
base_datos     = st.secrets["postgres"].dbname
usuario        = st.secrets["postgres"].user
contrasena     = st.secrets["postgres"].password
puerto         = st.secrets["postgres"].port

col1 = st.columns(1)

# Boton para subir los archivos de datos
listado_archivos_subidos = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
for archivo_subido in listado_archivos_subidos:

    ## Lectura de los datos subidos
    # Programa PELACUS   
    if id_programa_elegido == 1: 
        try:
            datos       = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_subido)
            texto_exito = 'Lectura del archivo ' + archivo_subido.name + ' realizada correctamente'
            st.success(texto_exito)
        except:
            texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")

    # Programa Radiales (2-Vigo, 3-Coruña, 4-Santander)    
    if id_programa_elegido == 2 or id_programa_elegido == 3 or id_programa_elegido == 4: 
        try:    
            datos = FUNCIONES_INSERCION.lectura_datos_radiales(archivo_subido,direccion_host,base_datos,usuario,contrasena,puerto)
            texto_exito = 'Lectura del archivo ' + archivo_subido.name + ' realizada correctamente'
            st.success(texto_exito)
        except:
            texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")
    
    ## Realiza un control de calidad primario a los datos importados   
    try:
        datos = FUNCIONES_INSERCION.control_calidad(datos,archivo_variables_base_datos) 
        texto_exito = 'Control de calidad de los datos del archivo ' + archivo_subido.name + ' realizado correctamente'
        st.success(texto_exito)
    except:
        texto_error = 'Error en el control de calidad de los datos del archivo ' + archivo_subido.name
        st.warning(texto_error, icon="⚠️")

    ## Introduce los datos en la base de datos
    # try:
 
    with st.spinner('Insertando datos en la base de datos'):

        FUNCIONES_INSERCION.inserta_datos(datos,programa_elegido,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)
    
    texto_exito = 'Datos del archivo ' + archivo_subido.name + ' insertados en la base de datos correctamente'
    st.success(texto_exito)
        
    # except:
    #     texto_error = 'Error en la subida de los datos del archivo ' + archivo_subido.name
    #     st.warning(texto_error, icon="⚠️")
        
    # # Actualiza estado
    # try:
    #     FUNCIONES_INSERCION.actualiza_estado(datos,fecha_actualizacion,id_programa_elegido,programa_elegido,tipo_dato_elegido,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
    #     texto_exito = 'Las fechas de procesado contenidas en la base de datos han sido actualizadas correctamente'
    #     st.success(texto_exito)    
    # except:
    #     texto_error = 'Error al actualizar las fechas de procesado en la base de datos'
    #     st.warning(texto_error, icon="⚠️")    
    
    # del(datos)
        
    # texto = 'inicia 1 ' + (datetime.datetime.now()).strftime('%H:%M:%S')
    # st.text(texto)

    # datos = FUNCIONES_INSERCION.evalua_estaciones(datos_corregidos,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)
 
    # texto = 'estaciones ' + (datetime.datetime.now()).strftime('%H:%M:%S')
    # st.text(texto)

    # datos = FUNCIONES_INSERCION.evalua_registros(datos,programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)

    # texto = 'registros ' + (datetime.datetime.now()).strftime('%H:%M:%S')
    # st.text(texto)
    

 
    
  
  
    
### Despliega un formulario para elegir el programa y el tipo de información a insertar

# # Listado del tipo de dato a introducir      
# listado_opciones = ['Análisis de laboratorio','Procesado o revisión de datos ya disponibles']

# with st.form("Formulario insercion"):
#     st.write("Selecciona el origen y tipo de los datos a insertar")
#     programa_elegido  = st.selectbox('Selecciona el programa al que corresponden los datos a insertar',(df_programas['nombre_programa']))
#     tipo_dato_elegido = st.selectbox('Selecciona el origen de los datos a insertar', (listado_opciones))
#     email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")

#     # Botón de envío para confirmar selección
#     submitted = st.form_submit_button("Enviar")
#     if submitted:
#         st.write("Resultado de la selección. Programa: ", programa_elegido, ". Tipo de dato: ", tipo_dato_elegido)
  
    
  
    
  
    
  
    
  
  