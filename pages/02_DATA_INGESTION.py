# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import streamlit as st
import sqlite3
import pandas 
from io import BytesIO

# # DATOS COMUNES
# base_datos     = 'IEO_Coruna'
# usuario        = 'postgres'
# contrasena     = 'IEO2022'
# puerto         = '5432'
# direccion_host = 'localhost'

#base_datos = 'DATOS/COAC.db'
base_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/PRUEBA_WEB/prueba/DATOS/COAC.db'

dir_base = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/PRUEBA_WEB/prueba/'

logo_IEO_reducido  = dir_base + 'DATOS/IMAGENES/ieo.ico'
archivo_plantilla  = dir_base + 'DATOS/PLANTILLA.xlsx'

# ### Encabezados y titulos 
titulo = 'Portal para la entrada de datos' 
st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
st.title(titulo)

# Recupera los programas disponibles en la base de datos
conn = sqlite3.connect(base_datos)
cursor = conn.cursor()
instruccion_sql = "SELECT id_programa,nombre_programa FROM programas;"
cursor.execute(instruccion_sql)
datos_programas =cursor.fetchall()
conn.commit()
cursor.close()
conn.close()

### Genera los listados de selección de los datos

# Listado de los programas disponibles
listado_programas = [None]*len(datos_programas)
for iprograma in range(len(datos_programas)):
    listado_programas[iprograma] = datos_programas[iprograma][1]

# Listado del tipo de dato a introducir      
listado_opciones = ['Análisis de laboratorio','Procesado o revisión de datos disponibles']




### Despliega un formulario de selección de origen

with st.form("Formulario insercion"):
    st.write("Selecciona el origen y tipo de los datos a insertar")
    programa_elegido  = st.selectbox('Selecciona el programa al que corresponden los datos a insertar',(listado_programas))
    tipo_dato_elegido = st.selectbox('Selecciona el origen de los datos a insertar', (listado_opciones))

    # Botón de envío para confirmar selección
    submitted = st.form_submit_button("Enviar")
    if submitted:
        st.write("Resultado de la selección. Programa: ", programa_elegido, ". Tipo de dato: ", tipo_dato_elegido)


### Recupera los identificadores de la selección hecha

# Encuentra el identificador asociado al programa
for iprograma in range(len(datos_programas)):
    if listado_programas[iprograma] == programa_elegido:
        id_programa_elegido = iprograma

# Encuentra el identificador asociado al tipo de dato
for iorigen in range(len(listado_opciones)):
    if listado_opciones[iorigen] == tipo_dato_elegido:
        id_opcion_elegida = iorigen





### Subida de archivos

# Boton para subir los archivos de datos
uploaded_files = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
for uploaded_file in uploaded_files:
     bytes_data = uploaded_file.read()
     st.write("Archivo subido:", uploaded_file.name)
     st.write(bytes_data)










### Recordatorio de formato

# Despliega un recordatorio de ajustar los datos a un formato y un botón para descargar una plantilla
st.write('')
st.warning('Los archivos a subir deben ajustarse a la plantilla disponible más abajo', icon="⚠️")
st.write('')

## Botón para descargar la plantilla
datos_plantilla = pandas.read_excel(archivo_plantilla, 'DATOS')

output = BytesIO()
writer = pandas.ExcelWriter(output, engine='xlsxwriter')
datos_plantilla.to_excel(writer, index=False, sheet_name='DATOS')
workbook = writer.book
worksheet = writer.sheets['DATOS']
writer.save()
datos_exporta = output.getvalue()

st.download_button(
    label="DESCARGAR PLANTILLA",
    data=datos_exporta,
    file_name='Plantilla_datos.xlsx',
    help= 'Descarga un archivo .xlsx de referencia para subir los datos solicitados',
    mime="application/vnd.ms-excel"
)
