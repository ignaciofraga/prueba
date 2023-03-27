# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 11:56:00 2022

@author: ifraga
"""
import pandas
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import numpy


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# recupera la información de los muestreos incluidos en la base de datos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
df_salidas       = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
df_estaciones    = psql.read_sql('SELECT * FROM estaciones', conn_psql)
conn_psql.dispose()


archivo_condiciones = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/AMBIENTALES/AMBIENTALES.xlsx'

datos_ambientales   = pandas.read_excel(archivo_condiciones, 'condiciones ambientales')

# Quita la primera fila, con datos de unidades
datos_ambientales = datos_ambientales.iloc[1: , :]

# Convierte las fechas al formato correspondiente
for idato in range(datos_ambientales.shape[0]):
    datos_ambientales['Fecha '].iloc[idato] = datos_ambientales['Fecha '].iloc[idato].date()
         
# Cambia los nans por None
datos_ambientales = datos_ambientales.replace(numpy.nan, None)
    
# Une con el df de las salidas, para que ya se queden sólo los datos que tienen una salida asociada    
datos_ambientales = datos_ambientales.rename(columns={"Fecha ": "fecha_salida"}) # Para igualar los nombres de columnas                                               
datos_ambientales = pandas.merge(datos_ambientales, df_salidas, on="fecha_salida")

# Encuentra la estación asignada
datos_ambientales['id_estacion'] = numpy.zeros(datos_ambientales.shape[0],dtype=int)
for idato in range(datos_ambientales.shape[0]):
    datos_ambientales['id_estacion'].iloc[idato] = df_estaciones['id_estacion'][df_estaciones['nombre_estacion']==str(datos_ambientales['Estación'].iloc[idato]).upper()].iloc[0]

# Ajusta el texto de lluvia
for idato in range(datos_ambientales.shape[0]):
    if datos_ambientales['Lluvia'].iloc[idato] == 'si' or datos_ambientales['Lluvia'].iloc[idato] == 'SI' or datos_ambientales['Lluvia'].iloc[idato] == 'Si' or datos_ambientales['Lluvia'].iloc[idato] == 'sI':
        datos_ambientales['Lluvia'].iloc[idato] = 'Si'
    elif datos_ambientales['Lluvia'].iloc[idato] == 'no' or datos_ambientales['Lluvia'].iloc[idato] == 'NO' or datos_ambientales['Lluvia'].iloc[idato] == 'No' or datos_ambientales['Lluvia'].iloc[idato] == 'nO':
        datos_ambientales['Lluvia'].iloc[idato] = 'No'
    else:
        datos_ambientales['Lluvia'].iloc[idato] = None
    
# Calcula el estado de Beaufort y Douglas
# datos_ambientales['Beaufort'] = None

# beaufort_nombre = ['Calma (0)','Ventolina (1)','Flojito (2)','Flojo (3)','Moderada (4)','Fresquito (5)','Fresco (6)','Frescachón (7)','Temporal (8)','Temporal fuerte (9)']
# beaufort_vmin   = [0  ,0.2, 1.5, 3.3, 5.4, 7.90, 10.70 ,13.8, 17.1, 20.7 ]
# beaufort_vmax   = [0.2,1.5, 3.3, 5.4, 7.9, 10.7, 13.80, 17.1, 20.7, 24.5 ]

# for idato in range(datos_ambientales.shape[0]):   
#     if datos_ambientales['Viento'].iloc[idato] is not None:
#         for iescala in range(len(beaufort_nombre)) :
#             if datos_ambientales['Viento'].iloc[idato] > beaufort_vmin [iescala] and datos_ambientales['Viento'].iloc[idato] < beaufort_vmax [iescala]:
#                 datos_ambientales['Beaufort'].iloc[idato] = beaufort_nombre[iescala]

datos_ambientales['Douglas']  = None
douglas_nombre = ['Mar rizada (1)','Marejadilla (2)', 'Marejada (3)', 'Fuerte marejada (4)', 'Gruesa (5)', 'Muy Gruesa (6)']
douglas_vmin   = [0  , 0.1 , 0.50 , 1.25, 2.5, 4]
douglas_vmax   = [0.1, 0.5 , 1.25 , 2.50, 4.0, 6]

for idato in range(datos_ambientales.shape[0]):   
    if datos_ambientales['Altura de Ola'].iloc[idato] is not None:
        for iescala in range(len(douglas_nombre)) :
            if datos_ambientales['Altura de Ola'].iloc[idato] > douglas_vmin [iescala] and datos_ambientales['Altura de Ola'].iloc[idato] < douglas_vmax [iescala]:
                datos_ambientales['Douglas'].iloc[idato] = douglas_nombre[iescala]



instruccion_sql = '''INSERT INTO condiciones_ambientales_muestreos (salida,estacion,hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,estado_mar,mar_direccion,humedad_relativa,temp_aire,prof_secchi,max_clorofila,marea,temp_superficie)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (salida,estacion) DO UPDATE SET (hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,estado_mar,mar_direccion,humedad_relativa,temp_aire,prof_secchi,max_clorofila,marea,temp_superficie) = ROW(EXCLUDED.hora_llegada,EXCLUDED.profundidad,EXCLUDED.nubosidad,EXCLUDED.lluvia,EXCLUDED.velocidad_viento,EXCLUDED.direccion_viento,EXCLUDED.pres_atmosferica,EXCLUDED.altura_ola,EXCLUDED.mar_fondo,EXCLUDED.estado_mar,EXCLUDED.mar_direccion,EXCLUDED.humedad_relativa,EXCLUDED.temp_aire,EXCLUDED.prof_secchi,EXCLUDED.max_clorofila,EXCLUDED.marea,EXCLUDED.temp_superficie);''' 
    

conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()     
    
# Introduce datos en la base de datos
for idato in range(datos_ambientales.shape[0]):          
    cursor.execute(instruccion_sql, (int(datos_ambientales['id_salida'].iloc[idato]),int(datos_ambientales['id_estacion'].iloc[idato]),datos_ambientales['Hora inicio'].iloc[idato],datos_ambientales['Profundidad'].iloc[idato],datos_ambientales['Nubosidad'].iloc[idato],datos_ambientales['Lluvia'].iloc[idato],datos_ambientales['Viento'].iloc[idato],datos_ambientales['Viento_dir'].iloc[idato],datos_ambientales['Presíon'].iloc[idato],datos_ambientales['Altura de Ola'].iloc[idato],datos_ambientales['Mar de fondo'].iloc[idato],datos_ambientales['Douglas'].iloc[idato],datos_ambientales['Mar'].iloc[idato],datos_ambientales['Humedad'].iloc[idato],datos_ambientales['Tª aire'].iloc[idato],datos_ambientales['Secchi'].iloc[idato],datos_ambientales['Max. Cla'].iloc[idato],datos_ambientales['Marea'].iloc[idato],datos_ambientales['Tª superf.'].iloc[idato]))
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

    conn.commit()
cursor.close()
conn.close()

# if io_previo == 0:
#     texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' añadidos correctamente'
# if io_previo == 1:
#     texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' actualizados correctamente'
    
# st.success(texto_exito)                

