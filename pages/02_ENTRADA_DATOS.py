# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import streamlit as st
import psycopg2
import pandas 
from io import BytesIO
import pandas.io.sql as psql

from pages.COMUNES import FUNCIONES_INSERCION

# DATOS BASE

logo_IEO_reducido     =  'DATOS/IMAGENES/ieo.ico'
archivo_plantilla     =  'DATOS/PLANTILLA.xlsx'
archivo_instrucciones =  'DATOS/INSTRUCCIONES_PLANTILLA.zip'


##### FUNCIONES AUXILIARES ######

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

##### WEB STREAMLIT #####



# ### Encabezados y titulos 
st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
st.title('Servicio de entrada de datos en la base de datos del C.O de A Coruña')

# Recupera la tabla de los programas disponibles en la base de datos, como un dataframe
conn = init_connection()
df_programas = psql.read_sql('SELECT * FROM programas', conn)
conn.close()

st.text(st.secrets["postgres"])

# Listado del tipo de dato a introducir      
listado_opciones = ['Análisis de laboratorio','Procesado o revisión de datos ya disponibles']

# Despliega un formulario para elegir el programa y el tipo de información a insertar

with st.form("Formulario insercion"):
    st.write("Selecciona el origen y tipo de los datos a insertar")
    programa_elegido  = st.selectbox('Selecciona el programa al que corresponden los datos a insertar',(df_programas['nombre_programa']))
    tipo_dato_elegido = st.selectbox('Selecciona el origen de los datos a insertar', (listado_opciones))

    # Botón de envío para confirmar selección
    submitted = st.form_submit_button("Enviar")
    if submitted:
        st.write("Resultado de la selección. Programa: ", programa_elegido, ". Tipo de dato: ", tipo_dato_elegido)


### Recupera los identificadores de la selección hecha

# Recupera el identificador del programa seleccionado
id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

# Encuentra el identificador asociado al tipo de dato
for iorigen in range(len(listado_opciones)):
    if listado_opciones[iorigen] == tipo_dato_elegido:
        id_opcion_elegida = iorigen


# ### Subida de archivos

# # Boton para subir los archivos de datos
# uploaded_files = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
# for uploaded_file in uploaded_files:
#     bytes_data = uploaded_file.read()
#     st.write("Archivo subido:", uploaded_file.name)
#     st.write(bytes_data)

#     if id_programa_elegido == 1:
        
#         datos = lectura_datos_pelacus(nombre_archivo,base_datos,usuario,contrasena,puerto)

#     # Lectura de los archivos subidos
#     print('Leyendo los datos contenidos en el archivo excel')
#     datos_radiales = FUNCIONES_INSERCION.lectura_datos_radiales(nombre_archivo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    
    
#     # Realiza un control de calidad primario a los datos importados   
#     print('Realizando control de calidad')
#     datos_radiales_corregido = FUNCIONES_INSERCION.control_calidad(datos_radiales,archivo_variables_base_datos)  
     
#     # Recupera el identificador del programa de muestreo
#     id_programa = FUNCIONES_INSERCION.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
#     # Introduce los datos en la base de datos
#     print('Introduciendo los datos en la base de datos')
#     FUNCIONES_INSERCION.inserta_datos(datos_radiales_corregido,min_dist,programa_muestreo,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
          
#     # Actualiza estado
#     print('Actualizando el estado de los procesos')
#     FUNCIONES_INSERCION.actualiza_estado(datos_radiales_corregido,id_programa,programa_muestreo,itipo_informacion,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
    
    
    













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


with open(archivo_instrucciones, "rb") as fp:
    btn = st.download_button(
        label="DESCARGA INTRUCCIONES",
        data=fp,
        file_name="Instrucciones.zip",
        help= 'Descarga un archivo con instrucciones para rellenar la plantilla de datos',
        mime="application/zip"
    )    


