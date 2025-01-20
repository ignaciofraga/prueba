# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 08:27:43 2023

@author: ifraga
"""

import pandas
import datetime
import pandas.io.sql as psql
from sqlalchemy import create_engine
import FUNCIONES_PROCESADO
import psycopg2

pandas.options.mode.chained_assignment = None


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

fecha_umbral = datetime.date(2018,1,1)

# Recupera la tabla con los registros de los muestreos
con_engine    = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn          = create_engine(con_engine)
df_muestreos  = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
df_datos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
conn.dispose() 
 


# listado_variables_datos  = df_datos.columns.tolist()
# #listado_variables_datos.remove('tubo_nutrientes')
# datos_tubo_nutrientes    = df_datos.loc[df_datos['tubo_nutrientes'].notnull(), listado_variables_datos]


# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()
# for idato in range(datos_tubo_nutrientes.shape[0]):

#     instruccion_sql = "UPDATE muestreos_discretos SET tubo_nutrientes = %s WHERE muestreo = %s;"                
#     cursor.execute(instruccion_sql, (str(datos_tubo_nutrientes['tubo_nutrientes'].iloc[idato]),int(datos_tubo_nutrientes['muestreo'].iloc[idato])))
#     conn.commit()
# cursor.close()

# conn.close()

                    


