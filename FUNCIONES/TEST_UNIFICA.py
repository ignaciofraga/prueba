# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 08:27:43 2023

@author: ifraga
"""
import numpy
import pandas
import datetime
from pyproj import Proj
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import seawater
import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import seawater

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

fecha_umbral = datetime.date(2018,1,1)

# Recupera la tabla con los registros de los muestreos
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn        = create_engine(con_engine)
df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
df_datos_biogeoquimicos   = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
df_datos_fisicos          = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
df_datos                  = psql.read_sql('SELECT * FROM datos_discretos', conn)
df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
df_programas              = psql.read_sql('SELECT * FROM programas', conn)
df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
conn.dispose()   

#df_datos_disponibles  = pandas.merge(df_muestreos, df_datos_fisicos, on="muestreo")

#FUNCIONES_PROCESADO.inserta_datos(df_datos_disponibles,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)



#df_datos_disponibles  = pandas.merge(df_datos_biogeoquimicos, df_muestreos, on="muestreo")

#FUNCIONES_PROCESADO.inserta_datos(df_datos_disponibles,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)





# Define las variables a utilizar
variables_procesado    = ['Temperatura','Salinidad','PAR','Fluorescencia','O2(CTD)']    
variables_procesado_bd = ['temperatura_ctd','salinidad_ctd','par_ctd','fluorescencia_ctd','oxigeno_ctd']
variables_unidades     = ['ºC','psu','\u03BCE/m2.s1','\u03BCg/kg','\u03BCmol/kg']
variable_tabla         = ['discreto_fisica','discreto_fisica','discreto_fisica','discreto_bgq','discreto_bgq']
variable_tabla         = ['datos_discretos_fisica','datos_discretos_fisica','datos_discretos_fisica','datos_discretos_biogeoquimica','datos_discretos_biogeoquimica']

   

## Toma los datos de la caché    
#df_muestreos,df_estaciones,df_datos_biogeoquimicos,df_datos_fisicos,df_salidas,df_programas,df_indices_calidad = carga_datos_entrada_archivo_roseta()

# Mantén sólo las salidas de radiales
id_radiales   = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
df_salidas  = df_salidas[df_salidas['programa']==int(id_radiales)]
       

# Combina la información de muestreos y salidas en un único dataframe 
df_muestreos          = df_muestreos.rename(columns={"salida_mar": "id_salida"}) # Para igualar los nombres de columnas                                               
df_muestreos          = pandas.merge(df_muestreos, df_salidas, on="id_salida")
df_muestreos          = df_muestreos.rename(columns={"id_salida": "salida_mar"}) # Deshaz el cambio de nombre
                 


# compón un dataframe con la información de muestreo y datos biogeoquímicos                                            
df_datos_disponibles  = pandas.merge(df_datos_biogeoquimicos, df_muestreos, on="muestreo")
df_datos_disponibles  = pandas.merge(df_datos_disponibles, df_datos_fisicos, on="muestreo")
 


# Añade columna con información del año
df_datos_disponibles['año'] = pandas.DatetimeIndex(df_datos_disponibles['fecha_muestreo']).year

## Borra los dataframes que ya no hagan falta para ahorrar memoria
#del(df_datos_biogeoquimicos,df_datos_fisicos,df_muestreos)

# procesa ese dataframe
io_control_calidad = 1
#indice_programa,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada,salida_seleccionada = FUNCIONES_AUXILIARES.menu_seleccion(df_datos_disponibles,variables_procesado,variables_procesado_bd,io_control_calidad,df_salidas,df_estaciones,df_programas)
indice_programa       = 3
indice_estacion       = 1
indice_salida         = 3682
cast_seleccionado     = 1
meses_offset          = 1
#variable_seleccionada = 'temperatura_ctd'
variable_seleccionada = 'par_ctd'                                      

df_datos_disponibles_store = df_datos_disponibles


# Recupera el nombre "completo" de la variable y sus unidades
indice_variable          = variables_procesado_bd.index(variable_seleccionada)
nombre_completo_variable = variables_procesado[indice_variable] 
unidades_variable        = variables_unidades[indice_variable]
tabla_insercion          = variable_tabla[indice_variable]
                                                                      
# Selecciona los datos correspondientes al programa, estación, salida y cast seleccionados
datos_procesados     = df_datos_disponibles[(df_datos_disponibles["programa"] == indice_programa) & (df_datos_disponibles["estacion"] == indice_estacion) & (df_datos_disponibles["salida_mar"] == indice_salida) & (df_datos_disponibles["num_cast"] == cast_seleccionado)]

df_datos_disponibles = df_datos_disponibles[(df_datos_disponibles["programa"] == indice_programa) & (df_datos_disponibles["estacion"] == indice_estacion)]
    



