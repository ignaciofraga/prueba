# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""



import pandas 

from pages.COMUNES.FUNCIONES_AUXILIARES import pagina_programa

pandas.options.mode.chained_assignment = None


# Carga el modulo con las funciones comunes
#import FUNCIONES_AUXILIARES

# Carga los parámetros de la base de datos y de estilo
archivo_parametros = 'DATOS/DATOS_GENERALES.xlsx'
xls                = pandas.ExcelFile(archivo_parametros)
df_base_datos      = pandas.read_excel(xls, 'BASE_DATOS')
df_estilos         = pandas.read_excel(xls, 'ESTILOS')

# Parámetros de la base de datos
base_datos = df_base_datos['nombre'][0]
usuario    = df_base_datos['usuario'][0]
contrasena = df_base_datos['contrasena'][0]
puerto     = str(df_base_datos['puerto'][0])

# Estilos
listado_estados = df_estilos['Estado']
listado_colores = df_estilos['Color']


# Llama a la funcion que construye la página
pagina_programa('PELACUS',listado_estados,listado_colores,base_datos,usuario,contrasena,puerto)




