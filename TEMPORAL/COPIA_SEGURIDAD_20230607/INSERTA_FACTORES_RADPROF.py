# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""







import FUNCIONES_LECTURA
import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
import datetime
import psycopg2
import numpy


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADPROF'
archivo_factores  = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADPROF/DATOS_MONICA/Factores_postcrossover.xlsx'

# Abre el archivo para leer los factores
datos_factores = pandas.read_excel(archivo_factores)

# Predimensiona
listado_variables = ['f_nitrato_x2','f_nitrato_x','f_nitrato_to','f_silicato_x2','f_silicato_x','f_silicato_to','f_fosfato_x2','f_fosfato_x','f_fosfato_to']
for ivariable in range(len(listado_variables)):
    datos_factores[listado_variables[ivariable]]=[None]*datos_factores.shape[0]

for idato in range(datos_factores.shape[0]):
    
    if type(datos_factores['NITRATO'][idato]) == float:
        datos_factores['f_nitrato_x'][idato] = datos_factores['NITRATO'][idato]
        datos_factores['f_nitrato_x2'][idato] = 0
        datos_factores['f_nitrato_to'][idato] = 0
    
    if datos_factores['NITRATO'][idato] == 'NO':
        datos_factores['f_nitrato_x'][idato]  = 1
        datos_factores['f_nitrato_x2'][idato] = 0
        datos_factores['f_nitrato_to'][idato] = 0
        
    if type(datos_factores['SILICATO'][idato]) == float:
        datos_factores['f_silicato_x'][idato] = datos_factores['SILICATO'][idato]
        datos_factores['f_silicato_x2'][idato] = 0
        datos_factores['f_silicato_to'][idato] = 0
    
    if datos_factores['SILICATO'][idato] == 'NO':
        datos_factores['f_silicato_x'][idato]  = 1
        datos_factores['f_silicato_x2'][idato] = 0
        datos_factores['f_silicato_to'][idato] = 0
        
    if type(datos_factores['FOSFATO'][idato]) == float:
        datos_factores['f_fosfato_x'][idato] = datos_factores['FOSFATO'][idato]
        datos_factores['f_fosfato_x2'][idato] = 0
        datos_factores['f_fosfato_to'][idato] = 0
    
    if datos_factores['FOSFATO'][idato] == 'NO':
        datos_factores['f_fosfato_x'][idato]  = 1
        datos_factores['f_fosfato_x2'][idato] = 0
        datos_factores['f_fosfato_to'][idato] = 0


        
# Asocia la salida correspondiente
datos_factores['nombre_salida']=[None]*datos_factores.shape[0]
datos_factores['salida_mar']   =[None]*datos_factores.shape[0]

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

for idato in range(datos_factores.shape[0]): 
    datos_factores['nombre_salida'][idato] = programa_muestreo + ' ' + str(datos_factores['AÑO'][idato])  
    
                
    # Recupera el identificador de la salida al mar
    instruccion_sql_recupera = "SELECT id_salida FROM salidas_muestreos WHERE nombre_salida = '" + datos_factores['nombre_salida'][idato] + "';"
    cursor.execute(instruccion_sql_recupera)
    id_salida =cursor.fetchone()[0]
    datos_factores['salida_mar'][idato] = id_salida
    conn.commit()
    
cursor.close()
conn.close()   

datos_exporta = datos_factores[['salida_mar','nombre_salida','f_nitrato_x','f_nitrato_x2','f_nitrato_to','f_silicato_x','f_silicato_x2','f_silicato_to','f_fosfato_x','f_fosfato_x2','f_fosfato_to']]

# # Define un nuevo índice de filas. Si se han eliminado registros este paso es necesario
# indices_dataframe      = numpy.arange(0,datos_exporta.shape[0],1,dtype=int)    
# datos_exporta['index'] = indices_dataframe
# datos_exporta.set_index('index',drop=True,append=False,inplace=True)


# Recupera la tabla con los registros de los muestreos
from sqlalchemy import create_engine

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)

datos_exporta.to_sql('factores_correctores_nutrientes', conn_psql,if_exists='append') 

#     if datos_factores['SILICATO'][idato] == 'NO':
#         datos_factores['SILICATO'][idato] = 1
        
#     if datos_factores['FOSFATO'][idato] == 'NO':
#         datos_factores['FOSFATO'][idato] = 1
        
        # ' f_nitrato_x2 NUMERIC (6, 4) NOT NULL,'
        # ' f_nitrato_x NUMERIC (6, 4) NOT NULL,'
        # ' f_nitrato_to NUMERIC (6, 4) NOT NULL,'
        # ' f_silicato_x2 NUMERIC (6, 4) NOT NULL,'
        # ' f_silicato_x NUMERIC (6, 4) NOT NULL,'
        # ' f_silicato_to NUMERIC (6, 4) NOT NULL,'
        # ' f_fosfato_x2 NUMERIC (6, 4) NOT NULL,'
        # ' f_fosfato_x NUMERIC (6, 4) NOT NULL,'
        # ' f_fosfato_to NUMERIC (6, 4) NOT NULL,'
        # ' observaciones json'