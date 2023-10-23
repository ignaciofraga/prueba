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
import pickle

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_seleccionado = 'RADPROF'
tipo_salida           = 'ANUAL'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
#archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 
archivo_datos                ='C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADPROF2023/BTL_RADPROF23.xlsx'
archivo_guardar              ='C:/Users/ifraga/Desktop/03-DESARROLLOS/NUTRIENTES/PROCESADO/RADPROF2023/BTL_RADPROF23_pickle'

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'



###### PROCESADO ########


# df_datos_importacion  = pandas.read_excel(archivo_datos) 


# # Recupera el identificador del programa de muestreo
# id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra la estación asociada a cada registro
# datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(df_datos_importacion,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra las salidas al mar correspondientes  
# nombre_entrada = programa_seleccionado
# datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,nombre_entrada,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 


# # Recupera la tabla con los registros de los muestreos
# con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql        = create_engine(con_engine)
# tabla_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
# tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
# tabla_variables  = psql.read_sql('SELECT * FROM variables_procesado', conn_psql)
# conn_psql.dispose() 


#listado_salidas  = datos_corregidos['id_salida'].unique()

#df_datos_salidas = tabla_muestreos[tabla_muestreos['salida_mar'].isin(listado_salidas)]
import time
st = time.time()

# # Encuentra el identificador asociado a cada registro
# datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
# et = time.time()    
        
# with open(archivo_guardar, 'wb') as handle:
#     pickle.dump(datos_corregidos, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open(archivo_guardar, 'rb') as handle:
    datos_corregidos = pickle.load(handle)


#FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)


datos_insercion=datos_corregidos


tabla_destino = 'datos_discretos'
puntero       = 'muestreo'

# Recupera la tabla con las variables disponibles
con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql          = create_engine(con_engine)
tabla_variables    = psql.read_sql('SELECT * FROM variables_procesado', conn_psql) 
conn_psql.dispose()
    
# Lee las variables de cada tipo a utilizar en el control de calidad
df_variables = tabla_variables[tabla_variables['tipo']=='variable_muestreo']
variables_bd = df_variables['nombre']    
  
# Busca qué variables están incluidas en los datos a importar
listado_variables_datos   = datos_insercion.columns.tolist()
listado_variables_comunes = list(set(listado_variables_datos).intersection(variables_bd))
listado_adicional         = [puntero] + listado_variables_comunes


# Genera la intrucción de escritura
str_var = ','.join(listado_adicional)
str_com = ','.join(listado_variables_comunes)
listado_exc = ['EXCLUDED.' + s for s in listado_variables_comunes]
str_exc = ','.join(listado_exc)
listado_str = ['%s']*len(listado_adicional)
str_car = ','.join(listado_str)

instruccion_sql = 'INSERT INTO ' + tabla_destino + '(' + str_var + ') VALUES (' + str_car + ') ON CONFLICT (' + puntero + ') DO UPDATE SET (' + str_com + ') = ROW(' + str_exc +');'
  
# Genera un dataframe sólo con las variables a insertar y el puntero
datos_variables = datos_insercion[listado_adicional]

# Convierte todos los datos a formato nativo de python
df_formato  = pandas.DataFrame(index=range(datos_variables.shape[0]),columns=listado_adicional)
for idato in range(df_formato.shape[0]):
    for ivar in range(df_formato.shape[1]):
        try:
            df_formato.iloc[idato].iloc[ivar] = (datos_variables.iloc[idato].iloc[ivar]).item()
        except:
            pass
        
# Conecta con la base de datos
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor() 

for idato in range(df_formato.shape[0]): # Dataframe con la interseccion de los datos nuevos y los disponibles en la base de datos, a partir de la variable muestreo
     
    # Inserta los datos
    cursor.execute(instruccion_sql,(df_formato.iloc[idato]))
    conn.commit()       

cursor.close()
conn.close()
    
    # ON CONFLICT (nombre_rmn) DO UPDATE SET (salinidad,ton,nitrito,silicato,fosfato,observaciones) = ROW(EXCLUDED.salinidad,EXCLUDED.ton,EXCLUDED.nitrito,EXCLUDED.silicato,EXCLUDED.fosfato,EXCLUDED.observaciones);'
        
    
    # nombre_perfil = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(datos_muestreo_perfil['cast_muestreo'].iloc[0])
    
    # conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    # cursor = conn.cursor()    
    # cursor.execute(instruccion_sql,(nombre_perfil,int(id_estacion),int(id_salida),int(datos_muestreo_perfil['cast_muestreo'].iloc[0]),datos_muestreo_perfil['fecha_muestreo'].iloc[0],datos_muestreo_perfil['hora_muestreo'].iloc[0],datos_muestreo_perfil['lon_muestreo'].iloc[0],datos_muestreo_perfil['lat_muestreo'].iloc[0]))
    # conn.commit() 

    # instruccion_sql = "SELECT perfil FROM perfiles_verticales WHERE nombre_perfil = '" + nombre_perfil + "';" 
    # cursor = conn.cursor()    
    # cursor.execute(instruccion_sql)
    # id_perfil =cursor.fetchone()[0]
    # conn.commit()       
    
    # cursor.close()
     





dt = time.time()

        
# texto_exito = 'Datos del archivo ' + archivo_datos.name + ' añadidos correctamente a la base de datos'
# #st.success(texto_exito)


