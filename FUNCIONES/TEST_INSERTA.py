# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import FUNCIONES_AUXILIARES
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_seleccionado = 'OTROS'
tipo_salida           = 'PUNTUAL'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/PRUEBAS STREAMLIT/BTL_acuario_TEST.xlsx'

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

fecha_actualizacion = datetime.date.today()

###### PROCESADO ########


con_engine      = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn            = create_engine(con_engine)
df_programas              = psql.read_sql('SELECT * FROM programas', conn)
variables_bd              = psql.read_sql('SELECT * FROM variables_procesado', conn)
conn.dispose() 



df_datos_importacion  = pandas.read_excel(archivo_datos) 

# Identifica las variables que contiene el archivo
variables_archivo = df_datos_importacion.columns.tolist()
variables_fisica  = list(set(variables_bd['variables_fisicas']).intersection(variables_archivo))
variables_bgq     = list(set(variables_bd['variables_biogeoquimicas']).intersection(variables_archivo))
        
        
# Corrige el formato de las fechas
for idato in range(df_datos_importacion.shape[0]):
    df_datos_importacion['fecha_muestreo'][idato] = (df_datos_importacion['fecha_muestreo'][idato]).date()           
    if df_datos_importacion['fecha_muestreo'][idato]:
        if 'hora_muestreo' in variables_archivo and isinstance(df_datos_importacion['hora_muestreo'][idato], str):
            df_datos_importacion['hora_muestreo'][idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'][idato], '%H:%M:%S').time()

# Cambia el nombre del identificador 
try:
    df_datos_importacion = df_datos_importacion.rename(columns={"ID": "id_externo"})
except:
    texto_aviso = "Los datos importados no contienen identificador."
    #st.warning(texto_aviso, icon="⚠️")



# Realiza un control de calidad primario a los datos importados   
datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion,direccion_host,base_datos,usuario,contrasena,puerto)  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)



# Encuentra la estación asociada a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# Encuentra las salidas al mar correspondientes  
programa_seleccionado = 'MUESTRAS ACUARIO'
datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,programa_seleccionado,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 
# Encuentra el identificador asociado a cada registro
datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
     
# # Añade datos físicos
# if len(variables_fisica)>0:
        
#     FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)

# # Añade datos biogeoquímicos
# if len(variables_bgq)>0:

FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)

        
# texto_exito = 'Datos del archivo ' + archivo_datos.name + ' añadidos correctamente a la base de datos'
# #st.success(texto_exito)


