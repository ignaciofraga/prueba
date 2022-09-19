# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""


import streamlit as st
import pandas 
from io import BytesIO
import numpy
import psycopg2

import pandas.io.sql as psql

# from pages.COMUNES import FUNCIONES_AUXILIARES

pandas.options.mode.chained_assignment = None


logo_IEO_reducido  = 'DATOS/IMAGENES/ieo.ico'


##### FUNCIONES AUXILIARES ######

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


##### WEB STREAMLIT #####

# if FUNCIONES_AUXILIARES.check_password():
    
### Encabezados y titulos 
st.set_page_config(page_title='CONSULTA ESTADILLOS', layout="wide",page_icon=logo_IEO_reducido) 
st.title('Servicio de consulta de estadillos de datos muestreados')

# Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
direccion_host = st.secrets["postgres"].host
base_datos     = st.secrets["postgres"].dbname
usuario        = st.secrets["postgres"].user
contrasena     = st.secrets["postgres"].password
puerto         = st.secrets["postgres"].port


# Recupera las tablas de los programas y estaciones disponibles como  dataframes
conn = init_connection()
df_programas  = psql.read_sql('SELECT * FROM programas', conn)
df_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
conn.close()


# Selecciona el programa del que se quieren buscar estadillos
nombre_programa  = st.selectbox('Selecciona el programa del cual se quiere recuperar el estadillo',(df_programas['nombre_programa']))

id_programa      = int(df_programas['id_programa'][df_programas['nombre_programa']==nombre_programa].values[0])


# Determina las fechas de las que hay información de datos de nutrientes
estaciones_programa = df_estaciones[df_estaciones['programa'] == id_programa]

indices_dataframe   = numpy.arange(0,estaciones_programa.shape[0],1,dtype=int) 

# # Primero recupera los registros correspondientes al periodo evaluado y al año consultado
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)

cursor = conn.cursor()
instruccion_sql = "SELECT id_muestreo,nombre_muestreo,fecha_muestreo,hora_muestreo,estacion,botella,profundidad,id_tubo_nutrientes FROM muestreos_discretos INNER JOIN estaciones ON muestreos_discretos.estacion = estaciones.id_estacion WHERE estaciones.programa = %s;"
cursor.execute(instruccion_sql,(str(id_programa)))
registros_consulta = cursor.fetchall()
conn.commit()
cursor.close()
conn.close()


dataframe_registros = pandas.DataFrame(registros_consulta, columns=['id_muestreo','nombre_muestreo','fecha_muestreo','hora_muestreo','estacion','botella','profundidad','id_tubo_nutrientes'])

# Mantén sólo los registros con datos de id_nutrientes
dataframe_registros = dataframe_registros[dataframe_registros['id_tubo_nutrientes'].notna()]

# Busca las fechas disponibles 
dataframe_temporal = dataframe_registros.drop_duplicates('fecha_muestreo')
listado_fechas     = dataframe_temporal['fecha_muestreo']

if len(listado_fechas) > 0:

    # Seleccionas una fecha
    fecha_seleccionada = st.selectbox('Selecciona la fecha de la que se quiere recuperar el estadillo',(listado_fechas))
    
    # Recupera los registros correspondientes a esa fecha
    dataframe_fecha = dataframe_registros[dataframe_registros['fecha_muestreo']==fecha_seleccionada]
    
    # Ajusta el numero de los indices
    indices_dataframe          = numpy.arange(0,dataframe_fecha.shape[0],1,dtype=int)    
    dataframe_fecha['id_temp'] = indices_dataframe
    dataframe_fecha.set_index('id_temp',drop=True,append=False,inplace=True)
    
    # Recupera las coordenadas a partir de la estación asignada
    dataframe_fecha['latitud'] = numpy.zeros(dataframe_fecha.shape[0])
    dataframe_fecha['longitud'] = numpy.zeros(dataframe_fecha.shape[0])
    for idato in range(dataframe_fecha.shape[0]):
        dataframe_fecha['latitud'][idato]  = estaciones_programa['latitud'][estaciones_programa['id_estacion']==dataframe_fecha['estacion'][idato]]
        dataframe_fecha['longitud'][idato] = estaciones_programa['longitud'][estaciones_programa['id_estacion']==dataframe_fecha['estacion'][idato]]
    
    # Recupera las propiedades físicas del registro (temperatura, salinidad....)
    conn = init_connection()
    tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
    conn.close()
    dataframe_fecha['temperatura_ctd'] = numpy.zeros(dataframe_fecha.shape[0])
    dataframe_fecha['salinidad_ctd'] = numpy.zeros(dataframe_fecha.shape[0])
    for idato in range(dataframe_fecha.shape[0]):
        dataframe_fecha['temperatura_ctd'][idato]  = tabla_registros_fisica['temperatura_ctd'][tabla_registros_fisica['muestreo']==dataframe_fecha['id_muestreo'][idato]]
        dataframe_fecha['salinidad_ctd'][idato]    = tabla_registros_fisica['salinidad_ctd'][tabla_registros_fisica['muestreo']==dataframe_fecha['id_muestreo'][idato]]
    
    # Quita la columna de estación
    dataframe_fecha = dataframe_fecha.drop(columns=['estacion','id_muestreo'])
    
    # Ajusta el orden de las columnas
    dataframe_fecha = dataframe_fecha[['nombre_muestreo','fecha_muestreo','hora_muestreo','latitud','longitud','botella','id_tubo_nutrientes','profundidad','temperatura_ctd','salinidad_ctd']]
    
    # Ordena en función del número de tubo
    dataframe_fecha = dataframe_fecha.sort_values(by=['id_tubo_nutrientes'])
   
    ## Botón para exportar los resultados
    nombre_archivo =  'ESTADILLO_' + nombre_programa + fecha_seleccionada.strftime("%m/%d/%Y") + '.xlsx'

    output = BytesIO()
    writer = pandas.ExcelWriter(output, engine='xlsxwriter')
    dataframe_fecha.to_excel(writer, index=False, sheet_name='DATOS')
    workbook = writer.book
    worksheet = writer.sheets['DATOS']
    writer.save()
    datos_exporta = output.getvalue()

    st.download_button(
        label="DESCARGA LOS DATOS SELECCIONADOS",
        data=datos_exporta,
        file_name=nombre_archivo,
        help= 'Descarga un archivo .csv con el estadillo solicitado',
        mime="application/vnd.ms-excel"
    )
    
else:
    
    texto_error = 'No hay estadillos de entrada correspondientes al programa ' + nombre_programa
    st.warning(texto_error, icon="⚠️")




