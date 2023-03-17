# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 08:15:20 2022

@author: ifraga
"""

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

import numpy
import pandas
import datetime
import numpy
import pandas
import datetime
from pyproj import Proj
import math
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine
import json
import re
import seawater

import FUNCIONES_PROCESADO
import FUNCIONES_LECTURA


programa_seleccionado = 'RADIAL CORUÑA'
tipo_salida           = 'MENSUAL'
abreviatura_programa  = 'RADCOR'


archivo_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/PRUEBA_WEB/prueba/EJEMPLOS/EXCEL_ENTRADA.xlsx'

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
datos_fisica              = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)
muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
conn_psql.dispose()

df_disponibles            = pandas.merge(datos_fisica, muestreos, on="muestreo")

datos_fisica['densidad'] = seawater.eos80.dens0(datos_fisica['salinidad_ctd'], datos_fisica['temperatura_ctd'])


df_disponibles['densidad2'] = seawater.eos80.dens(df_disponibles['salinidad_ctd'], df_disponibles['temperatura_ctd'], df_disponibles['presion_ctd'])

    

