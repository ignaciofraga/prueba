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

# from pages.COMUNES import FUNCIONES_AUXILIARES
from pages.COMUNES import FUNCIONES_INSERCION

# DATOS BASE

logo_IEO_reducido            =  'DATOS/IMAGENES/ieo.ico'
archivo_plantilla            =  'DATOS/PLANTILLA.xlsx'
archivo_instrucciones        =  'DATOS/PLANTILLA.zip'

##### FUNCIONES AUXILIARES ######

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])




##### WEB STREAMLIT #####

# if FUNCIONES_AUXILIARES.check_password():

st.markdown("ENTRADA DE DATOS")
st.sidebar.markdown("ENTRADA DE DATOS")    

# Encabezados y titulos 
#st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
st.title('Servicio de entrada de datos del C.O de A Coruña')

# Recupera la tabla de los programas disponibles en la base de datos, como un dataframe
conn = init_connection()
df_programas = psql.read_sql('SELECT * FROM programas', conn)
conn.close()


# Despliega un formulario para elegir el programa y la fecha a consultar
with st.form("Formulario seleccion"):

    listado_opciones = ['Datos de NUTRIENTES procedentes de análisis de laboratorio','Datos de NUTRIENTES procesados o revisados','Datos de MUESTREOS ']
    tipo_dato_elegido = st.selectbox('Selecciona el tipo de información que se va a subir', (listado_opciones))
    
    col1, col2 = st.columns(2,gap="small")
    #nombre_programa, tiempo_consulta = st.columns((1, 1))
    with col1:
        programa_elegido  = st.selectbox('Selecciona el programa al que corresponde la información',(df_programas['nombre_programa']))
    with col2:
        email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")

    # Botón de envío para confirmar selección
    submitted = st.form_submit_button("Enviar")



### Recupera los identificadores de la selección hecha

# Recupera el identificador del programa seleccionado
id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

# Encuentra el identificador asociado al tipo de dato
for iorigen in range(len(listado_opciones)):
    if listado_opciones[iorigen] == tipo_dato_elegido:
        id_opcion_elegida = iorigen + 1
                
# Si se elige introducir un estadillo, recordar que se ajusten a la plantilla
if id_opcion_elegida ==3:
    texto_error = 'IMPORTANTE. Los datos a subir deben ajustarse a la plantilla facilitada' 
    st.warning(texto_error, icon="⚠️")

#    st.download_button('DESCARGAR PLANTILLA E INSTRUCCIONES', archivo_instrucciones, file_name='PLANTILLA.zip')        

    with open(archivo_instrucciones, "rb") as fp:
        btn = st.download_button(
            label="DESCARGAR PLANTILLA E INSTRUCCIONES",
            data=fp,
            file_name="PLANTILLA.zip",
            mime="application/zip"
        )
        
    
fecha_actualizacion = datetime.date.today()    
    
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

    
    # Opciones 1 y 2, lectura de datos de nutrientes
    if id_opcion_elegida == 1 or id_opcion_elegida == 2:
        ## Lectura de los datos subidos 
        # Programa PELACUS   
        if id_programa_elegido == 1: 
            try:
                datos       = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_subido)
                texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
                st.success(texto_exito)
            except:
                texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
                st.warning(texto_error, icon="⚠️")
    
        # Programa Radiales (2-Vigo, 3-Coruña, 4-Santander)    
        if id_programa_elegido == 2 or id_programa_elegido == 3 or id_programa_elegido == 4: 
            try:    
                datos = FUNCIONES_INSERCION.lectura_datos_radiales(archivo_subido,direccion_host,base_datos,usuario,contrasena,puerto)
                texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
                st.success(texto_exito)
            except:
                texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
                st.warning(texto_error, icon="⚠️")
        
    # Opcion 3, lectura de estadillo con datos de entrada
    if id_opcion_elegida == 3:
        try:
            datos,texto_error = FUNCIONES_INSERCION.lectura_datos_estadillo(archivo_subido,archivo_plantilla)
            texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
            st.success(texto_exito)
            if len(texto_error)>0:
                for iaviso in range(len(texto_error)):
                    st.warning(texto_error[iaviso], icon="⚠️")
        
        except:
            texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")


    # Realiza un control de calidad primario a los datos importados   
    try:
        datos,textos_aviso = FUNCIONES_INSERCION.control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto) 
        texto_exito = 'Control de calidad de los datos del archivo ' + archivo_subido.name + ' realizado correctamente'
        st.success(texto_exito)
        if len(textos_aviso)>0:
            for iaviso in range(len(textos_aviso)):
                st.warning(textos_aviso[iaviso], icon="⚠️")
        
    except:
        texto_error = 'Error en el control de calidad de los datos del archivo ' + archivo_subido.name
        st.warning(texto_error, icon="⚠️")

    # Introduce los datos en la base de datos
    # try:
 
    with st.spinner('Insertando datos en la base de datos'):

        datos = FUNCIONES_INSERCION.evalua_estaciones(datos,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)  

        datos = FUNCIONES_INSERCION.evalua_registros(datos,programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)

        FUNCIONES_INSERCION.inserta_datos_fisica(datos,direccion_host,base_datos,usuario,contrasena,puerto)

        FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos,direccion_host,base_datos,usuario,contrasena,puerto)
        
    texto_exito = 'Datos del archivo ' + archivo_subido.name + ' insertados en la base de datos correctamente'
    st.success(texto_exito)

    # except:
    #     texto_error = 'Error en la subida de los datos del archivo ' + archivo_subido.name
    #     st.warning(texto_error, icon="⚠️")
        
    # Actualiza estado
    try:
        
        FUNCIONES_INSERCION.actualiza_estado(datos,fecha_actualizacion,id_programa_elegido,programa_elegido,id_opcion_elegida,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
     
        texto_exito = 'Las fechas de procesado contenidas en la base de datos han sido actualizadas correctamente'
        st.success(texto_exito)    
    except:
        texto_error = 'Error al actualizar las fechas de procesado en la base de datos'
        st.warning(texto_error, icon="⚠️")    
 
  
    
  
        
  
    
  
  