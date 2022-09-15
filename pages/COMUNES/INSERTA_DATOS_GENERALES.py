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
import pandas.io.sql as psql
from sqlalchemy import create_engine

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
    instruccion_sql = "INSERT INTO programas (id_programa,nombre_programa,centro_asociado) VALUES (%s,%s,%s) ON CONFLICT (id_programa) DO UPDATE SET (nombre_programa,centro_asociado) = (EXCLUDED.nombre_programa, EXCLUDED.centro_asociado);"   
    cursor.execute(instruccion_sql, (iprograma+1,programas[iprograma],id_asociado[iprograma]))
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



### Información de las variables a utilizar en el procesado ###

parametros_muestreo = ['nombre_muestreo','fecha_muestreo','hora_muestreo','estacion','profundidad','botella','configuracion_perfilador','configuracion_superficie','programa','latitud','longitud','id_tubo_nutrientes']
var_biogeoquimica   = ['oxigeno_ctd','fluorescencia_ctd','oxigeno_wk','sio2','no3','no2','nh4','po4','clorofila_a','tcarbn','doc','cdom','alkali','phts25p0_unpur','phts25p0_pur','r_clor','r_per','co3_temp']
var_fisicas         = ['temperatura_ctd','salinidad_ctd','par_ctd','turbidez_ctd']

# compón un dataframe con todas las variables
dataframe_variables = pandas.DataFrame({'parametros_muestreo': pandas.Series(parametros_muestreo), 'variables_biogeoquimicas': pandas.Series(var_biogeoquimica), 'variables_fisicas': pandas.Series(var_fisicas)})

# Asigna índices
indices_dataframe              = numpy.arange(0,dataframe_variables.shape[0],1,dtype=int)    
dataframe_variables['id_variable'] = indices_dataframe
dataframe_variables.set_index('id_variable',drop =True,append=False,inplace=True)

con_engine                = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql                 = create_engine(con_engine)

dataframe_variables.to_sql('variables_procesado', conn_psql,if_exists='replace')




### AÑADE LOS REGISTROS DE ENTRADA DE LOS TIEMPOS/FECHAS DE LAS DIFERENTES CAMPAÑAS


archivo_fechas = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/TIEMPOS.xlsx'

con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
conn_psql        = create_engine(con_engine)
tabla_programas  = psql.read_sql('SELECT * FROM programas', conn_psql)

for iprograma in range(tabla_programas.shape[0]):
    
    try:
        
        fechas_programa = pandas.read_excel(archivo_fechas,sheet_name=tabla_programas['nombre_programa'][iprograma])
        
        fechas_programa.replace({numpy.nan: None}, inplace = True)
        
        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
        cursor = conn.cursor()   
        
        for idatos_programa  in range(fechas_programa.shape[0]):   
            # Inserta la información en la base de datos
            datos_insercion     = [int(tabla_programas['id_programa'][iprograma]),tabla_programas['nombre_programa'][iprograma],int(fechas_programa['año'][idatos_programa]),fechas_programa['fecha_muestreo'][idatos_programa],fechas_programa['fecha_entrada_datos'][idatos_programa],fechas_programa['fecha_analisis_laboratorio'][idatos_programa],fechas_programa['fecha_post_procesado'][idatos_programa],fechas_programa['contacto_muestreo'][idatos_programa],fechas_programa['contacto_entrada_datos'][idatos_programa],fechas_programa['contacto_analisis_laboratorio'][idatos_programa],fechas_programa['contacto_post_procesado'][idatos_programa]]
        
            instruccion_sql = "INSERT INTO estado_procesos (programa,nombre_programa,año,fecha_final_muestreo,fecha_entrada_datos,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_entrada_datos,contacto_analisis_laboratorio,contacto_post_procesado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,año) DO UPDATE SET (nombre_programa,fecha_final_muestreo,fecha_entrada_datos,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_entrada_datos,contacto_analisis_laboratorio,contacto_post_procesado) = (EXCLUDED.nombre_programa,EXCLUDED.fecha_final_muestreo,EXCLUDED.fecha_entrada_datos,EXCLUDED.fecha_analisis_laboratorio,EXCLUDED.fecha_post_procesado,EXCLUDED.contacto_muestreo,EXCLUDED.contacto_entrada_datos,EXCLUDED.contacto_analisis_laboratorio,EXCLUDED.contacto_post_procesado);"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit()
        
        cursor.close()
        conn.close()

    except:
        pass



# for iprograma in range(tabla_programas.shape[0]):
#     try:

#         fechas_programa = pandas.read_excel(archivo_fechas,sheet_name=tabla_programas['nombre_programa'][iprograma])
        
#         conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
#         cursor = conn.cursor()   
        
#         for idatos_programa  in range(fechas_programa.shape[0]):   
#             # Inserta la información en la base de datos
#             datos_insercion     = [int(tabla_programas['id_programa'][iprograma]),tabla_programas['nombre_programa'][iprograma],int(fechas_programa['año'][idatos_programa]),fechas_programa['fecha_muestreo'][idatos_programa],fechas_programa['fecha_analisis_laboratorio'][idatos_programa],fechas_programa['fecha_post_procesado'][idatos_programa]]
        
#             instruccion_sql = "INSERT INTO estado_procesos (programa,nombre_programa,año,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (programa,año) DO UPDATE SET (nombre_programa,fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado) = (EXCLUDED.nombre_programa,EXCLUDED.fecha_final_muestreo,EXCLUDED.fecha_analisis_laboratorio,EXCLUDED.fecha_post_procesado);"   
#             cursor.execute(instruccion_sql, (datos_insercion))
#             conn.commit()
        
#         cursor.close()
#         conn.close()

#     except:
#         pass








