# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""
# DATOS COMUNES
# base_datos     = 'IEO_Coruna'
# usuario        = 'postgres'
# contrasena     = 'IEO2022'
# puerto         = '5432'
# direccion_host = 'localhost'


base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

import psycopg2
import numpy
import pandas
import datetime


#### TABLAS CON INFORMACIÓN DE LAS CONDICIONES DE MUESTREO (EQUIV. METADATOS) ####


### Informacion de los centros oceanograficos ###

centros       = ['CORUNA','VIGO','SANTANDER']
identificador = [1,2,3]

instruccion_sql = '''INSERT INTO centros_oceanograficos (id_centro,nombre_centro)
    VALUES (%s,%s) ON CONFLICT (id_centro) DO UPDATE SET (nombre_centro) = ROW(EXCLUDED.nombre_centro);''' 
        
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()
for idato in range(len(centros)):
    cursor.execute(instruccion_sql, (identificador[idato],centros[idato]))
    conn.commit()
cursor.close()
conn.close()



### Informacion de los buques oceanograficos ###

buques       = ['LURA','ANGELES ALVARINO','RAMON MARGALEF']
codigo_buque = ['LU','AJ','RM']
id_buque     = [1,2,3]

instruccion_sql = '''INSERT INTO buques (id_buque,nombre_buque,codigo_buque)
    VALUES (%s,%s,%s) ON CONFLICT (id_buque) DO UPDATE SET (nombre_buque,codigo_buque) = ROW(EXCLUDED.nombre_buque,EXCLUDED.codigo_buque);''' 
        
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()
for idato in range(len(buques)):
    cursor.execute(instruccion_sql, (id_buque[idato],buques[idato],codigo_buque[idato]))
    conn.commit()
cursor.close()
conn.close()



### Información de los programas ###

programas       = ['PELACUS','RADIAL VIGO','RADIAL CORUÑA','RADIAL SANTANDER','RADPROF']
centro_asociado = ['CORUNA','VIGO','CORUNA','SANTANDER','CORUNA']
abreviatura     = ['PEL','RVG','RCOR','RSAN','RPROF']

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

# Encuentra los centros incluidos y el identificador correspondiente
instruccion_sql = "SELECT id_centro FROM centros_oceanograficos;"
cursor.execute(instruccion_sql)
id_centro =cursor.fetchall()
conn.commit()

instruccion_sql = "SELECT nombre_centro FROM centros_oceanograficos;"
cursor.execute(instruccion_sql)
nombre_centro =cursor.fetchall()
conn.commit()

id_asociado = numpy.zeros(len(centro_asociado))
for icentro in range(len(centro_asociado)):
    for icompara in range(len(id_centro)):
        if centro_asociado[icentro] == nombre_centro[icompara][0]:
           id_asociado[icentro] = id_centro[icompara][0]


# Inserta los datos de cada programa
for iprograma in range(len(programas)):
    instruccion_sql = "INSERT INTO programas (id_programa,nombre_programa,centro_asociado,abreviatura) VALUES (%s,%s,%s,%s) ON CONFLICT (id_programa) DO UPDATE SET (nombre_programa,centro_asociado,abreviatura) = (EXCLUDED.nombre_programa, EXCLUDED.centro_asociado, EXCLUDED.abreviatura);"   
    cursor.execute(instruccion_sql, (iprograma+1,programas[iprograma],id_asociado[iprograma],abreviatura[iprograma]))
    conn.commit()    






### Informacion con la configuracion del perfilador ###
### IMPORTANTE: REVISAR ESTOS DATOS. AÑADIDOS PARA COMENZAR A PROBAR LA BASE DE DATOS
### LA INFORMACION NO ES FIABLE! (10/08/2022)



id_buque              = [1,2,3,2]
centro_asociado       = ['CORUNA','CORUNA','CORUNA','CORUNA']
sensores_ctd          = ['SBE_25','SBE_25','SBE_25','SBE_25']
num_serie_ctd         = ['NSER001','NSER001','NSER001','NSER002']
propietario_CTD       = ['COAC','COAC','COAC','COAC'] 
sensor_fluorescencia  = ['TURNER SCUFA','WET LABS AFL','WET LABS AFL','SEAPOINT'] 

vector_tiempos        = pandas.date_range(datetime.date.today()-datetime.timedelta(days=len(id_buque)),datetime.date.today(),freq='d')
fecha_inicio_config   = datetime.date(1995,1,1)
fecha_calibracion_ctd = vector_tiempos

# Encuentra el identificador correspondiente a cada centro
instruccion_sql = "SELECT id_centro FROM centros_oceanograficos;"
cursor.execute(instruccion_sql)
id_centro =cursor.fetchall()
conn.commit()

instruccion_sql = "SELECT nombre_centro FROM centros_oceanograficos;"
cursor.execute(instruccion_sql)
nombre_centro =cursor.fetchall()
conn.commit()

id_asociado = numpy.zeros(len(centro_asociado))
for icentro in range(len(centro_asociado)):
    for icompara in range(len(id_centro)):
        if centro_asociado[icentro] == nombre_centro[icompara][0]:
           id_asociado[icentro] = id_centro[icompara][0]


# Inserta los datos de cada configuracion del perfilador
for iconfiguracion in range(len(sensores_ctd)):
#    print(int(id_buque[iconfiguracion]),id_asociado[iconfiguracion],fecha_inicio_config,sensores_ctd[iconfiguracion],num_serie_ctd[iconfiguracion],propietario_CTD[iconfiguracion],fecha_calibracion_ctd[iconfiguracion])#,sensor_fluorescencia[iconfiguracion])
    instruccion_sql = "INSERT INTO configuracion_perfilador (id_config_perfil,buque,centro_asociado,fecha_inicio,sensor_ctd,num_serie_ctd,propietario_ctd,fecha_calibracion_ctd,sensor_fluorescencia) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_config_perfil) DO UPDATE SET (buque,centro_asociado,fecha_inicio,sensor_ctd,num_serie_ctd,propietario_ctd,fecha_calibracion_ctd,sensor_fluorescencia) = (EXCLUDED.buque,EXCLUDED.centro_asociado,EXCLUDED.fecha_inicio,EXCLUDED.sensor_ctd,EXCLUDED.num_serie_ctd,EXCLUDED.propietario_ctd,EXCLUDED.fecha_calibracion_ctd,EXCLUDED.sensor_fluorescencia);"   
    cursor.execute(instruccion_sql, (iconfiguracion+1,int(id_buque[iconfiguracion]),id_asociado[iconfiguracion],fecha_inicio_config,sensores_ctd[iconfiguracion],num_serie_ctd[iconfiguracion],propietario_CTD[iconfiguracion],fecha_calibracion_ctd[iconfiguracion],sensor_fluorescencia[iconfiguracion]))
    conn.commit() 




### Informacion con la configuracion del muestreo en superficie ###
### IMPORTANTE: REVISAR ESTOS DATOS. AÑADIDOS PARA COMENZAR A PROBAR LA BASE DE DATOS
### LA INFORMACION NO ES FIABLE! (10/08/2022)

vector_tiempos      = pandas.date_range(datetime.date.today()-datetime.timedelta(days=2),datetime.date.today(),freq='d')
fecha_inicio_config = datetime.date(1995,1,1)

id_buque              = [1,2,3]
centro_asociado       = ['CORUNA','VIGO','SANTANDER']
sensores_tsg          = ['TSG_SBE','TSG_SBE','TSG_SBE']
num_serie_tsg         = ['NTSG001','NTSG001','NTSG001']
propietario_tsg       = ['COAC','COVIGO','UTM'] 
fecha_calibracion_tsg = vector_tiempos

# Encuentra el identificador correspondiente a cada centro
instruccion_sql = "SELECT id_centro FROM centros_oceanograficos;"
cursor.execute(instruccion_sql)
id_centro =cursor.fetchall()
conn.commit()

instruccion_sql = "SELECT nombre_centro FROM centros_oceanograficos;"
cursor.execute(instruccion_sql)
nombre_centro =cursor.fetchall()
conn.commit()

id_asociado = numpy.zeros(len(centro_asociado))
for icentro in range(len(centro_asociado)):
    for icompara in range(len(id_centro)):
        if centro_asociado[icentro] == nombre_centro[icompara][0]:
           id_asociado[icentro] = id_centro[icompara][0]


# Inserta los datos de cada configuracion del perfilador
for iconfiguracion in range(len(sensores_tsg)):
    instruccion_sql = "INSERT INTO configuracion_superficie (id_config_superficie,buque,centro_asociado,fecha_inicio,sensor_tsg,num_serie_tsg,propietario_tsg,fecha_calibracion_tsg) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_config_superficie) DO UPDATE SET (buque,centro_asociado,fecha_inicio,sensor_tsg,num_serie_tsg,propietario_tsg,fecha_calibracion_tsg) = (EXCLUDED.buque,EXCLUDED.centro_asociado,EXCLUDED.fecha_inicio,EXCLUDED.sensor_tsg,EXCLUDED.num_serie_tsg,EXCLUDED.propietario_tsg,EXCLUDED.fecha_calibracion_tsg);"   
    cursor.execute(instruccion_sql, (iconfiguracion+1,int(id_buque[iconfiguracion]),id_asociado[iconfiguracion],fecha_inicio_config,sensores_tsg[iconfiguracion],num_serie_tsg[iconfiguracion],propietario_tsg[iconfiguracion],fecha_calibracion_tsg[iconfiguracion]))
    conn.commit() 


cursor.close()
conn.close()





### Informacion de las estaciones de la radial mensual ###

nombre_estacion = ['E2CO','E3ACO','E3CCO','E3BCO','E4CO']
latitud         = [43.421667,43.406667,43.393333,43.388333,43.363333]
longitud        = [-8.436667,-8.416667,-8.4,-8.383333,-8.37]


instruccion_sql = '''INSERT INTO estaciones (nombre_estacion,programa,latitud,longitud)
    VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO UPDATE SET (nombre_estacion,programa,latitud,longitud) = ROW(EXCLUDED.nombre_estacion,EXCLUDED.programa,EXCLUDED.latitud,EXCLUDED.longitud);''' 
        
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()
for idato in range(len(nombre_estacion)):
    cursor.execute(instruccion_sql, (nombre_estacion[idato],int(2),latitud[idato],longitud[idato]))
    conn.commit()
cursor.close()
conn.close()





### Informacion de los usuarios y contraseñas de la aplicacion ###

usuarios       = ['COAC - Administrador','COAC - Laboratorio Nutrientes','COAC - Supervisión Nutrientes','COAC - Radiales','Usuario externo']
passwords      = ['IEO_2022','IEO_2022','IEO_2022','IEO_2022','2022_IEO']

instruccion_sql = '''INSERT INTO usuarios_app (nombre_usuario,password)
    VALUES (%s,%s) ON CONFLICT (nombre_usuario) DO UPDATE SET (password) = ROW(EXCLUDED.password);''' 
        
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()
for idato in range(len(usuarios)):
    cursor.execute(instruccion_sql, (usuarios[idato],passwords[idato]))
    conn.commit()
cursor.close()
conn.close()



### Informacion de las variables utilizadas en el control de calidad automático ###


parametros_muestreo      = ['nombre_muestreo','fecha_muestreo','hora_muestreo','estacion','num_cast','presion_ctd','prof_referencia','botella','configuracion_perfilador','configuracion_superficie','programa','latitud','longitud']
variables_biogeoquimicas = ['oxigeno_ctd','fluorescencia_ctd','oxigeno_wk','sio2','no3','no2','nh4','po4','clorofila_a','tcarbn','doc','cdom','alkali','phts25p0_unpur','phts25p0_pur','r_clor','r_per','co3_temp']
variables_fisicas        = ['temperatura_ctd','salinidad_ctd','par_ctd','turbidez_ctd']

index                    = numpy.arange(0,len(variables_biogeoquimicas)) 
datos_variables = pandas.DataFrame(
    {'index': pandas.Series(index),
     'parametros_muestreo': pandas.Series(parametros_muestreo),
     'variables_biogeoquimicas': pandas.Series(variables_biogeoquimicas),
     'variables_fisicas': pandas.Series(variables_fisicas)
    })



datos_variables = datos_variables.replace({numpy.nan: None})


instruccion_sql = '''INSERT INTO variables_procesado (id_variable,parametros_muestreo,variables_fisicas,variables_biogeoquimicas)
    VALUES (%s,%s,%s,%s) ON CONFLICT (id_variable) DO UPDATE SET (parametros_muestreo,variables_fisicas,variables_biogeoquimicas) = ROW(EXCLUDED.parametros_muestreo,EXCLUDED.variables_fisicas,EXCLUDED.variables_biogeoquimicas);''' 
        
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()
for idato in range(datos_variables.shape[0]):
    cursor.execute(instruccion_sql, (int(datos_variables['index'][idato]),datos_variables['parametros_muestreo'][idato],datos_variables['variables_fisicas'][idato],datos_variables['variables_biogeoquimicas'][idato]))
    conn.commit()
cursor.close()
conn.close()








### AÑADE LOS REGISTROS DE ENTRADA DE LOS TIEMPOS/FECHAS DE LAS DIFERENTES CAMPAÑAS

# vector_tiempos      = pandas.date_range(datetime.date.today()-datetime.timedelta(days=2),datetime.date.today(),freq='d')
# fecha_inicio_config = datetime.date(1995,1,1)

# id_buque              = [1,2,3]
# centro_asociado       = ['CORUNA','VIGO','SANTANDER']
# sensores_tsg          = ['TSG_SBE','TSG_SBE','TSG_SBE']
# num_serie_tsg         = ['NTSG001','NTSG001','NTSG001']
# propietario_tsg       = ['COAC','COVIGO','UTM'] 
# fecha_calibracion_tsg = vector_tiempos

# # Encuentra el identificador correspondiente a cada centro
# instruccion_sql = "SELECT id_centro FROM centros_oceanograficos;"
# cursor.execute(instruccion_sql)
# id_centro =cursor.fetchall()
# conn.commit()

# instruccion_sql = "SELECT nombre_centro FROM centros_oceanograficos;"
# cursor.execute(instruccion_sql)
# nombre_centro =cursor.fetchall()
# conn.commit()

# id_asociado = numpy.zeros(len(centro_asociado))
# for icentro in range(len(centro_asociado)):
#     for icompara in range(len(id_centro)):
#         if centro_asociado[icentro] == nombre_centro[icompara][0]:
#            id_asociado[icentro] = id_centro[icompara][0]


# # Inserta los datos de cada configuracion del perfilador
# for iconfiguracion in range(len(sensores_tsg)):
#     instruccion_sql = "INSERT INTO configuracion_superficie (id_config_superficie,buque,centro_asociado,fecha_inicio,sensor_tsg,num_serie_tsg,propietario_tsg,fecha_calibracion_tsg) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_config_superficie) DO UPDATE SET (buque,centro_asociado,fecha_inicio,sensor_tsg,num_serie_tsg,propietario_tsg,fecha_calibracion_tsg) = (EXCLUDED.buque,EXCLUDED.centro_asociado,EXCLUDED.fecha_inicio,EXCLUDED.sensor_tsg,EXCLUDED.num_serie_tsg,EXCLUDED.propietario_tsg,EXCLUDED.fecha_calibracion_tsg);"   
#     cursor.execute(instruccion_sql, (iconfiguracion+1,int(id_buque[iconfiguracion]),id_asociado[iconfiguracion],fecha_inicio_config,sensores_tsg[iconfiguracion],num_serie_tsg[iconfiguracion],propietario_tsg[iconfiguracion],fecha_calibracion_tsg[iconfiguracion]))
#     conn.commit() 


# cursor.close()
# conn.close()










