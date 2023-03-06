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
from sqlalchemy import create_engine
import pandas.io.sql as psql
import json
import matplotlib.pyplot as plt

# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'PELACUS'
id_config_sup     = 1
id_config_per     = 1

# Rutas de los archivos a importar  
archivo_datos                = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/PELACUS/PELACUS_2000_2021.xlsx' 

# Tipo de información a introducir
itipo_informacion = 2 # 1-nuevo muestreo 2-dato nuevo (analisis laboratorio)  3-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'

fecha_actualizacion = datetime.date.today()

###### PROCESADO ########


# Recupera la tabla con los registros de muestreos físicos
con_engine         = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn          = create_engine(con_engine)
df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
df_programas            = psql.read_sql('SELECT * FROM programas', conn)
df_perfiles             = psql.read_sql('SELECT * FROM perfiles_verticales', conn)
df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_perfil_fisica', conn)
df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_perfil_biogeoquimica', conn)
df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)  
conn.dispose()
    
id_salida_seleccionada = 1166

df_perfiles_seleccion  = df_perfiles[df_perfiles['salida_mar']==int(id_salida_seleccionada)]
df_datos_combinado     = pandas.merge(df_datos_fisicos, df_datos_biogeoquimicos, on="perfil")

# initialize list of lists
data       = [['2', '#b50000'], ['3A', '#70ba07'], ['3B','#0085b5'], ['3C', '#5b00b5'], ['4', '#9d9d9e']]
df_colores = pandas.DataFrame(data, columns=['estacion', 'color'])

listado_variables = ['temperatura_ctd','salinidad_ctd','par_ctd','fluorescencia_ctd','oxigeno_ctd']
listado_unidades  = ['(degC)','(PSU)','(\u03BCE/m2s)','(\u03BCg/L)','(\u03BCmol/kg)']
listado_titulos   = ['Temp.','Sal.','PAR','Fluor.','Oxigeno']

fig, axs = plt.subplots(1, len(listado_variables),sharey='all')

for iperfil in range(df_perfiles_seleccion.shape[0]):
    
    df_perfil   = df_datos_combinado[df_datos_combinado['perfil']==df_perfiles_seleccion['perfil'].iloc[iperfil]]
    
    id_estacion     = df_perfiles_seleccion['estacion'].iloc[iperfil]
    nombre_estacion = df_estaciones['nombre_estacion'][df_estaciones['id_estacion']==int(id_estacion)].iloc[0]
    color_estacion  = df_colores['color'][df_colores['estacion']==nombre_estacion].iloc[0]

    for ivariable in range(len(listado_variables)):
        str_datos   = df_perfil[listado_variables[ivariable]].iloc[0]
        json_datos  = json.loads(str_datos)
        df_datos    =  pandas.DataFrame.from_dict(json_datos)
      
        axs[ivariable].plot(df_datos[listado_variables[ivariable]],df_datos['presion_ctd'],linewidth=2,color=color_estacion,label=nombre_estacion)

# Ajusta parámetros de los gráficos
for igrafico in range(len(listado_variables)):
    texto_eje = listado_titulos[igrafico] + listado_unidades[igrafico] 
    axs[igrafico].set(xlabel=texto_eje)
    axs[igrafico].invert_yaxis()
    if igrafico == 0:
        axs[igrafico].set(ylabel='Presion (db)')
        
# Añade la leyenda
axs[2].legend(loc='upper center',bbox_to_anchor=(0.5, 1.1),ncol=len(listado_variables), fancybox=True,fontsize=7)

    # # Añade el nombre de cada punto
    # nombre_muestreos = [None]*df_seleccion.shape[0]
    # for ipunto in range(df_seleccion.shape[0]):     
    #     if df_seleccion['botella'].iloc[ipunto] is None:
    #         nombre_muestreos[ipunto] = 'Prof.' + str(int(df_seleccion['presion_ctd'].iloc[ipunto]))
    #     else:
    #         nombre_muestreos[ipunto] = 'Bot.' + str(int(df_seleccion['botella'].iloc[ipunto]))
    #     ax.annotate(nombre_muestreos[ipunto], (df_seleccion[variable_seleccionada].iloc[ipunto], df_seleccion['presion_ctd'].iloc[ipunto]))
            
 
    
    

# # Lee las variables de cada tipo a utilizar en el control de calidad
# variables_muestreo = [x for x in tabla_variables['parametros_muestreo'] if str(x) != 'None']
# variables_fisicas  = [x for x in tabla_variables['variables_fisicas'] if str(x) != 'None']    
# variables_biogeoquimicas  = [x for x in tabla_variables['variables_biogeoquimicas'] if str(x) != 'None'] 

# # Busca qué variables están incluidas en los datos a importar
# listado_variables_datos   = datos_fisica.columns.tolist()
# listado_variables_comunes = list(set(listado_variables_datos).intersection(variables_fisicas))
# listado_adicional         = ['muestreo'] + listado_variables_comunes

# # Recupera la tabla con los registros de muestreos físicos
# con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
# conn_psql                 = create_engine(con_engine)
# tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)

# # Genera un dataframe solo con las variales fisicas de los datos a importar 
# datos_fisica = datos_fisica.rename(columns={"id_muestreo": "muestreo"})

# # # Si no existe ningún registro en la base de datos, introducir todos los datos disponibles
# if tabla_registros_fisica.shape[0] == 0:
#     datos_fisica.set_index('muestreo',drop=True,append=False,inplace=True)
#     datos_fisica.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')
    
# # En caso contrario, comprobar qué parte de la información está en la base de datos
# else: 
    
#     for idato in range(datos_fisica.shape[0]): # Dataframe con la interseccion de los datos nuevos y los disponibles en la base de datos, a partir de la variable muestreo
     
#         df_temp  = tabla_registros_fisica[(tabla_registros_fisica['muestreo']==datos_fisica['muestreo'].iloc[idato])] 
        
#         if df_temp.shape[0]>0:  # Muestreo ya incluido en la base de datos
        
#             muestreo = df_temp['muestreo'].iloc[0]
            
#             for ivariable in range(len(listado_variables_comunes)): # Reemplazar las variables disponibles en el muestreo correspondiente
                    
#                 tabla_registros_fisica[listado_variables_comunes[ivariable]][tabla_registros_fisica['muestreo']==int(muestreo)] = datos_fisica[listado_variables_comunes[ivariable]][datos_fisica['muestreo']==int(muestreo)]

        
#         else: # Nuevo muestreo
                   
#             df_add = datos_fisica[datos_fisica['muestreo']==datos_fisica['muestreo'].iloc[idato]] # Genero un dataframe con cada línea de datos a añadir

#             df_add = df_add[listado_adicional] # Recorto para que tenga sólo las variables a añadir
        
#             tabla_registros_fisica = pandas.concat([tabla_registros_fisica, df_add]) # Combino ambos dataframes
        
#     tabla_registros_fisica.set_index('muestreo',drop=True,append=False,inplace=True)
    
#     # borra los registros existentes en la tabla (no la tabla en sí, para no perder tipos de datos y referencias)
#     conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#     cursor = conn.cursor()
#     instruccion_sql = "TRUNCATE datos_discretos_fisica;"
#     cursor.execute(instruccion_sql)
#     conn.commit()
#     cursor.close()
#     conn.close() 
    
#     # Inserta el dataframe resultante en la base de datos 
#     tabla_registros_fisica.to_sql('datos_discretos_fisica', conn_psql,if_exists='append')


# conn_psql.dispose() # Cierra la conexión con la base de datos 





# # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# 
# FUNCIONES_PROCESADO.inserta_datos_biogeoquimica(datos_pelacus_corregido,direccion_host,base_datos,usuario,contrasena,puerto)


# FUNCIONES_AUXILIARES.actualiza_estado(datos_pelacus_corregido,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)


print('Procesado terminado')





