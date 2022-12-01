# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 12:58:34 2022

@author: ifraga
"""
import os
import FUNCIONES_INSERCION
import pandas
pandas.options.mode.chained_assignment = None
import datetime

import os
import numpy
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import os
from glob import glob
import json
import re

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

ruta    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados/2022/rad_men_2022_07_12_0485/btl+PAR+flscufa/'
archivo = '20220712e2+PAR+flscufa.btl'


ruta    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/MENSUALES/Procesados/2022/rad_men_2022_10_04_0485/btl+PAR+flscufa+O2/'
archivo = '20221004e2+PAR+flscufa+O2.btl'

archivo_temporal = 'TEMPORAL.btl'
programa_elegido = 'RADIAL CORUÑA'

nombre_archivo = archivo

os.chdir(ruta)


#def lectura_btl(nombre_archivo,archivo,archivo_temporal,nombre_programa,direccion_host,base_datos,usuario,contrasena,puerto):
 
    
  
    
lectura_archivo = open(archivo, "r") 
f.close()
    
# recupera la información de las estaciones incluidas en la base de datos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
tabla_estaciones = psql.read_sql('SELECT * FROM estaciones', conn_psql)
df_programas = psql.read_sql('SELECT * FROM programas', conn_psql)
conn_psql.dispose()

id_programa_elegido = df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].iloc[0]

df_estaciones_radiales = tabla_estaciones[tabla_estaciones['programa']==id_programa_elegido]



# Identifica la estación a la que corresponde el archivo
posicion_separador = nombre_archivo.index('+')
nombre_estacion    = nombre_archivo[8:posicion_separador].upper() + 'CO'                
id_estacion        = df_estaciones_radiales['id_estacion'][df_estaciones_radiales['nombre_estacion']==nombre_estacion].iloc[0] 

# Identifica la fecha del muestreo
fecha_salida_texto = nombre_archivo[0:8]
fecha_salida       = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()


id_estacion              = tabla_estaciones['id_estacion'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
profundidades_referencia = tabla_estaciones['profundidades_referencia'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
lat_estacion             = tabla_estaciones['latitud'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]
lon_estacion             = tabla_estaciones['longitud'][tabla_estaciones['nombre_estacion']==nombre_estacion].iloc[0]


# Genera las listas en las que se guardarán los datos si éstos existen
datos_botella     = []
datos_salinidad   = []
datos_temperatura = []
datos_presion     = []
datos_PAR         = []
datos_fluor       = []
datos_O2          = []

# Lee el archivo .btl y escribe la información de las botellas en un archivo temporal
cast_muestreo = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
with open(archivo, "r") as file_input:
    for line in file_input:
        if line[0:1] == '#' or line[0:1] == '*':
            if line[0:8] == '** Time:': # Línea con hora del cast
                hora_muestreo = datetime.datetime.strptime(line[8:-1],'%H:%M').time()            
            if line[0:8] == '** Cast:': # Línea con el número de cast
                cast_muestreo = int(line[8:-1])
            if line[0:8] == '** Date:': # Línea con la fecha
                fecha_muestreo_archivo = line[8:-1]
                if fecha_muestreo_archivo is not None:
                    fecha_muestreo_archivo = datetime.datetime.strptime(fecha_muestreo_archivo, '%d/%m/%y').date()

        else:

            # Separa las cabeceras de las medidas de oxigeno si existen y están juntas 
            if 'Sbeox0VSbeox0Mm/Kg' in line: 
                line = line.replace('Sbeox0VSbeox0Mm/Kg', 'Sbeox0V Sbeox0Mm/Kg')
               
            datos_linea = line.split()
                
            if datos_linea[0] == 'Bottle': # Primera línea con las cabeceras

                # Encuentra los indices (posiciones) de cada variable, si ésta está incluida
                indice_botellas  = datos_linea.index("Bottle")

                try:
                    indice_salinidad = datos_linea.index("Sal00")            
                except:
                    indice_salinidad = None
               
                try: 
                   indice_presion   = datos_linea.index("PrSM")
                except:
                    indice_presion  = None      
                
                try:
                    indice_temp     = datos_linea.index("T090C")
                except:
                    indice_temp     = None
                    
                try:
                    indice_par      = datos_linea.index("Par")
                    io_par          = 1
                except:
                    indice_par      = None
                    io_par          = 0
                  
                try:
                    indice_fluor    = datos_linea.index("FlScufa")  
                    io_fluor        = 1
                except:
                    indice_fluor    =  None 
                    io_fluor        = 0                    
                
                try:
                    indice_O2       = datos_linea.index("Sbeox0Mm/Kg")
                    io_O2           = 1                   
                except:
                    indice_O2       =  None  
                    io_O2           = 0     


            elif datos_linea[0] == 'Position': # Segunda línea con las cabeceras
                datos_linea = line.split() 
                
            else:  # Líneas con datos
                
                datos_linea = line.split()
                
                if datos_linea[-1] == '(avg)': # Línea con los registros de cada variable
                        
                    # Salvo en el caso del identificador de las botellas, sumar dos espacios al índice de cada variable
                    # porque la fecha la divide en 3 lecturas debido al espacio que contiene
                    
                    datos_botella.append(int(datos_linea[indice_botellas]))
                    datos_salinidad.append(float(datos_linea[indice_salinidad + 2])) 
                    datos_temperatura.append(float(datos_linea[indice_temp + 2]))
                    datos_presion.append(round(float(datos_linea[indice_presion + 2]),2))
                

                
                    if io_par == 1:
                        datos_PAR.append(float(datos_linea[indice_par + 2]))
                    if io_fluor == 1:
                        datos_fluor.append(float(datos_linea[indice_fluor + 2]))
                    if io_O2 == 1:
                        datos_O2.append(float(datos_linea[indice_O2 + 2]))                    
                                
file_input.close()


# Comprueba que la fecha contenida en el archivo y la del nombre del archivo son la misma
if fecha_muestreo_archivo is not None and fecha_muestreo_archivo != fecha_salida:

    mensaje_error  = 'La fecha indicada en el nombre del archivo no coincide con la que figura en dentro del mismo'
    datos_botellas = None

else:
    
    # Une las listas en un dataframe
    datos_botellas = pandas.DataFrame(list(zip(datos_botella, datos_salinidad,datos_temperatura,datos_presion)),
                   columns =['botella', 'salinidad_ctd','temperatura_ctd','presion_ctd'])
    
    # Añade columnas con el QF de T y S
    datos_botellas['temperatura_ctd_qf'] = 2
    datos_botellas['salinidad_ctd_qf']   = 2
    
    if io_par == 1:
        datos_botellas['par_ctd']              =  datos_PAR
        datos_botellas['par_ctd_qf']           = 2
    if io_fluor == 1:
        datos_botellas['fluorescencia_ctd']    =  datos_fluor
        datos_botellas['fluorescencia_ctd_qf'] = 2
    if io_O2 == 1:
        datos_botellas['oxigeno_ctd']          =  datos_O2
        datos_botellas['oxigeno_ctd_qf']       = 2
        
        
    # Añade una columna con la profundidad de referencia
    if profundidades_referencia is not None:
        datos_botellas['prof_referencia'] = numpy.zeros(datos_botellas.shape[0],dtype=int)
        for idato in range(datos_botellas.shape[0]):
                # Encuentra la profundidad de referencia más cercana a cada dato
                idx = (numpy.abs(profundidades_referencia - datos_botellas['presion_ctd'][idato])).argmin()
                datos_botellas['prof_referencia'][idato] =  profundidades_referencia[idx]
    else:
        datos_botellas['prof_referencia'] = [None]*datos_botellas.shape[0]
    
    
    # Añade informacion de lat/lon y fecha para que no elimine el registro durante el control de calidad
    datos_botellas['latitud']                  = lat_estacion  
    datos_botellas['longitud']                 = lon_estacion
    datos_botellas['fecha_muestreo']           = fecha_salida
    datos_botellas,textos_aviso                = FUNCIONES_INSERCION.control_calidad(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)            
    
    # Añade columnas con datos del muestreo 
    datos_botellas['id_estacion_temp']         = numpy.zeros(datos_botellas.shape[0],dtype=int)
    datos_botellas['id_estacion_temp']         = id_estacion
    datos_botellas['estacion']                 = id_estacion
    datos_botellas['fecha_muestreo']           = fecha_salida
    datos_botellas['hora_muestreo']            = hora_muestreo
    datos_botellas['num_cast']                 = cast_muestreo
    datos_botellas['configuracion_perfilador'] = 1
    datos_botellas['configuracion_superficie'] = 1
    datos_botellas['programa']                 = id_programa_elegido
    
    mensaje_error = []

#return mensaje_error,datos_botellas
    

#mensaje_error,datos_botellas = lectura_btl(nombre_archivo,archivo,archivo_temporal,programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)
 


# Añade indices de calidad a temp y salinidad




# if io_fluor == 1: 
#     datos_botellas['fluorescencia_ctd_qf'] = 2

# if io_o2 == 1: 
#     datos_botellas['oxigeno_ctd_qf']       = 2
 
# if io_par == 1: 
#     datos_botellas['par_ctd_qf']       = 2

     
# # Funcion
# # Lee el archivo .btl y escribe la información de las botellas en un archivo temporal
# cast_muestreo = 1 # Asinga este valor por si no se introdujo ningún dato en el muestreo
# with open(archivo, "r") as file_input:
#     with open(archivo_temporal, "w") as output:
#         for line in file_input:
#             if line[0:8] == '** Time:': # Línea con hora del cast
#                 hora_muestreo = datetime.datetime.strptime(line[8:-1],'%H:%M').time()            
#             if line[0:8] == '** Cast:': # Línea con el número de cast
#                 cast_muestreo = int(line[8:-1])
#             if line[-6:-1] == '(avg)': # Línea con datos de botella, escribir en el archivo temporal
#                 output.write(line)

# file_input.close()
# output.close()

# # Lee el archivo temporal como  un dataframe
# datos_botellas = pandas.read_csv(archivo_temporal, sep='\s+',header=None)

           
# # Elimina las columnas que no interesan y cambia los nombres
# if io_par == 1 and io_fluor == 0: 
#     datos_botellas = datos_botellas.drop(columns=[1,2,3,5,8,10])
#     datos_botellas = datos_botellas.rename(columns={0: 'botella', 4: 'salinidad_ctd',6:'presion_ctd',7:'temperatura_ctd',9:'par_ctd'})
#     # if i
# if io_par == 1 and io_fluor == 1 and io_o2 == 0:
#     datos_botellas = datos_botellas.drop(columns=[1,2,3,7,8,11]) 
#     datos_botellas = datos_botellas.rename(columns={0: 'botella', 4: 'salinidad_ctd',5:'presion_ctd',6:'temperatura_ctd',9:'par_ctd',10:'fluorescencia_ctd'})

# if io_par == 1 and io_fluor == 1 and io_o2 == 1:
#     datos_botellas = datos_botellas.drop(columns=[1,2,3,7,8,11,13,14]) 
#     datos_botellas = datos_botellas.rename(columns={0: 'botella', 4: 'salinidad_ctd',5:'presion_ctd',6:'temperatura_ctd',9:'par_ctd',10:'fluorescencia_ctd',12:'oxigeno_ctd'})
          
  
# # Elimina el archivo temporal
# os.remove(archivo_temporal)            

# # Añade una columna con la profundidad de referencia
# if profundidades_referencia is not None:
#     datos_botellas['prof_referencia'] = numpy.zeros(datos_botellas.shape[0],dtype=int)
#     for idato in range(datos_botellas.shape[0]):
#             # Encuentra la profundidad de referencia más cercana a cada dato
#             idx = (numpy.abs(profundidades_referencia - datos_botellas['presion_ctd'][idato])).argmin()
#             datos_botellas['prof_referencia'][idato] =  profundidades_referencia[idx]
# else:
#     datos_botellas['prof_referencia'] = [None]*datos_botellas.shape[0]

# # Redondea la precisión de los datos de profundidad
# datos_botellas['presion_ctd'] = round(datos_botellas['presion_ctd'],2)
    

# # Añade informacion de lat/lon y fecha para que no elimine el registro durante el control de calidad
# datos_botellas['latitud']                  = lat_estacion  
# datos_botellas['longitud']                 = lon_estacion
# datos_botellas['fecha_muestreo']           = fecha_salida
# datos_botellas,textos_aviso                = FUNCIONES_INSERCION.control_calidad(datos_botellas,direccion_host,base_datos,usuario,contrasena,puerto)            

# # Añade columnas con datos del muestreo 
# datos_botellas['id_estacion_temp']         = numpy.zeros(datos_botellas.shape[0],dtype=int)
# datos_botellas['id_estacion_temp']         = id_estacion
# datos_botellas['estacion']                 = id_estacion
# datos_botellas['fecha_muestreo']           = fecha_salida
# datos_botellas['hora_muestreo']            = hora_muestreo
# datos_botellas['num_cast']                 = cast_muestreo
# datos_botellas['configuracion_perfilador'] = 1
# datos_botellas['configuracion_superficie'] = 1
# datos_botellas['programa']                 = id_programa_elegido

# # Añade indices de calidad a temp y salinidad
# datos_botellas['temperatura_ctd_qf'] = 2
# datos_botellas['salinidad_ctd_qf']   = 2

# if io_fluor == 1: 
#     datos_botellas['fluorescencia_ctd_qf'] = 2

# if io_o2 == 1: 
#     datos_botellas['oxigeno_ctd_qf']       = 2
 
# if io_par == 1: 
#     datos_botellas['par_ctd_qf']       = 2    