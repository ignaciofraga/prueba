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
import numpy

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_seleccionado = 'RADIAL CORUÑA'
tipo_salida           = 'MENSUAL'
###### PROCESADO ########


con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn               = create_engine(con_engine)
df_programas       = psql.read_sql('SELECT * FROM programas', conn)
variables_bd       = psql.read_sql('SELECT * FROM variables_procesado', conn)
df_salidas         = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
df_muestreos       = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
conn.dispose() 


# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)


df_salidas_programa = df_salidas[(df_salidas['programa']==id_programa) & (df_salidas['tipo_salida']==tipo_salida)]

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

for isalida in range(df_salidas_programa.shape[0]):

    #for isalida in range(1):
    print(isalida)
        
    num_salida = df_salidas_programa['id_salida'].iloc[isalida]
    
    muestreos_salida = df_muestreos[df_muestreos['salida_mar']==num_salida]
    
    listado_estaciones = muestreos_salida['estacion'].unique()
    
    for iestacion in range(len(listado_estaciones)):
        
        datos_estacion = muestreos_salida[muestreos_salida['estacion']==listado_estaciones[iestacion]]
        
        datos_estacion = datos_estacion.replace({numpy.nan:None}) 
        
        datos_estacion = datos_estacion.sort_values(by=['presion_ctd'])

        if listado_estaciones[iestacion] == 1:
            lat_estacion = 43.4217
            lon_estacion = -8.4367
            nombre_estacion = 'E2CO'
            
        if listado_estaciones[iestacion] == 2:
            lat_estacion = 43.4067
            lon_estacion = -8.4167   
            nombre_estacion = 'E3ACO'
            
        if listado_estaciones[iestacion] == 3:
            lat_estacion = 43.3933
            lon_estacion = -8.4000  
            nombre_estacion = 'E3CCO'                  

        if listado_estaciones[iestacion] == 4:
            lat_estacion = 43.3883
            lon_estacion = -8.3833 
            nombre_estacion = 'E3BCO'
            
        if listado_estaciones[iestacion] == 5:
            lat_estacion = 43.3633
            lon_estacion = -8.3700
            nombre_estacion = 'E4CO'
            
        if listado_estaciones[iestacion] == 356:
            lat_estacion = 43.4033
            lon_estacion = -8.4116
            nombre_estacion = 'E3CO'

        instruccion_sql_botella = 'UPDATE muestreos_discretos SET botella =%s WHERE muestreo = %s;'
        instruccion_sql_lat     = 'UPDATE muestreos_discretos SET latitud_muestreo =%s WHERE muestreo = %s;'
        instruccion_sql_lon     = 'UPDATE muestreos_discretos SET longitud_muestreo =%s WHERE muestreo = %s;'  
        instruccion_sql_nombre  = 'UPDATE muestreos_discretos SET nombre_muestreo =%s WHERE muestreo = %s;' 
        for idato in range(datos_estacion.shape[0]):
                
            cursor.execute(instruccion_sql_botella, (int(idato+1),int(datos_estacion['muestreo'].iloc[idato])))
            conn.commit()
            
            cursor.execute(instruccion_sql_lat, (lat_estacion,int(datos_estacion['muestreo'].iloc[idato])))
            conn.commit()
            
            cursor.execute(instruccion_sql_lon, (lon_estacion,int(datos_estacion['muestreo'].iloc[idato])))
            conn.commit()
            
            if datos_estacion['num_cast'].iloc[idato] is not None:
                nombre_muestreo = 'RADCOR_'+ datos_estacion['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_' + nombre_estacion + '_C' + str(int(datos_estacion['num_cast'].iloc[idato])) + '_B' + str(int(idato+1))
            else:
                nombre_muestreo = 'RADCOR_'+ datos_estacion['fecha_muestreo'].iloc[idato].strftime("%Y%m%d") + '_' + nombre_estacion + '_B' + str(int(idato+1))
                
            cursor.execute(instruccion_sql_nombre, (nombre_muestreo,int(datos_estacion['muestreo'].iloc[idato])))
            conn.commit()

cursor.close()
conn.close()



#  4 43.3633   8.3700
#  2 43.4217   8.4367
#  

# 1	"E2CO"	3	43.4217	-8.4367
# 2	"E3ACO"	3	43.4067	-8.4167
# 3	"E3CCO"	3	43.3933	-8.4000
# 4	"E3BCO"	3	43.3883	-8.3833
# 5	"E4CO"	3	43.3633	-8.3700
# 356	"E3CO"	3	43.4033	-8.4116	
# df_salidas_programa['año'] = None
# for idato in range(df_salidas_programa.shape[0]):
#     df_salidas_programa['año'].iloc[idato] = (df_salidas_programa['fecha_salida'].iloc[idato]).year

# df_salidas_seleccion = df_salidas_programa[df_salidas_programa['año']==anho_consulta]
# listado_salidas      = df_salidas_seleccion['id_salida']

# #listado_salidas = [81]


# df_muestreos_seleccionados = df_muestreos[df_muestreos['salida_mar'].isin(listado_salidas)]

# df_muestreos_seleccionados = df_muestreos_seleccionados.sort_values(by=['muestreo'])

# df_datos = pandas.merge(df_muestreos_seleccionados, df_datos_discretos, on="muestreo")

#df_datos = df_datos[df_datos['carbono_organico_total'].notna()]