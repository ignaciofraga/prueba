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
import numpy
import seawater


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

# Parámetros
programa_muestreo = 'RADIAL CORUÑA'

#archivo_variables_base_datos = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/VARIABLES.xlsx'  
directorio_datos             = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/HISTORICO'

itipo_informacion = 1 # 1-dato nuevo (analisis laboratorio)  2-dato re-analizado (control calidad)   
email_contacto    = 'prueba@ieo.csic.es'


# Define la fecha actual
fecha_actualizacion = datetime.date.today()

### PROCESADO ###


nombre_archivo = directorio_datos + '/HISTORICO_MODIFICADO.xlsx'
    
# Importa el .xlsx
datos_radiales = pandas.read_excel(nombre_archivo, 'datos')

# # Elimina la primera fila, con unidades de las distintas variables
# datos_radiales = datos_radiales.iloc[1: , :]

# Convierte las fechas de DATE a formato correcto
datos_radiales['Fecha'] =  pandas.to_datetime(datos_radiales['Fecha'], format='%Y%m%d').dt.date
  
# Define una columna índice
indices_dataframe         = numpy.arange(0,datos_radiales.shape[0],1,dtype=int)
datos_radiales['id_temp'] = indices_dataframe
datos_radiales.set_index('id_temp',drop=True,append=False,inplace=True)

# Cambia los nan por None
datos_radiales = datos_radiales.replace(numpy.nan, None)

# Corrige las concentraciones para pasarlas de mmol/m3 (o umol/l) a umol/kg

# Calculo de densidades
datos_radiales['densidad'] = numpy.ones(datos_radiales.shape[0])
for idato in range(datos_radiales.shape[0]):
    if datos_radiales['sigmat'].iloc[idato] is not None:
        datos_radiales['densidad'].iloc[idato]  =  1 + datos_radiales['sigmat'].iloc[idato]/10000

# correccion
datos_radiales['ton']      = [None]*datos_radiales.shape[0]
datos_radiales['nitrato']  = [None]*datos_radiales.shape[0]
datos_radiales['nitrito']  = [None]*datos_radiales.shape[0]
datos_radiales['amonio']   = [None]*datos_radiales.shape[0]
datos_radiales['fosfato']  = [None]*datos_radiales.shape[0]
datos_radiales['silicato'] = [None]*datos_radiales.shape[0]

datos_radiales['botella']       = [None]*datos_radiales.shape[0]
datos_radiales['hora_muestreo'] = [None]*datos_radiales.shape[0]
datos_radiales['prof_referencia'] = [None]*datos_radiales.shape[0]
datos_radiales['num_cast']        = [None]*datos_radiales.shape[0]

for idato in range(datos_radiales.shape[0]):
    if datos_radiales['NO3'].iloc[idato] is not None:
        datos_radiales['nitrato'].iloc[idato]  = datos_radiales['NO3'].iloc[idato]/datos_radiales['densidad'].iloc[idato]            
    if datos_radiales['NO2'].iloc[idato] is not None:
        datos_radiales['nitrito'].iloc[idato]  = datos_radiales['NO2'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
    if datos_radiales['NH4'].iloc[idato] is not None:
        datos_radiales['amonio'].iloc[idato]  = datos_radiales['NH4'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
    if datos_radiales['PO4'].iloc[idato] is not None:
        datos_radiales['fosfato'].iloc[idato]  = datos_radiales['PO4'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
    if datos_radiales['SiO2'].iloc[idato] is not None:
        datos_radiales['silicato'].iloc[idato]  = datos_radiales['SiO2'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  

    # calculo del TON 
    datos_radiales['ton'].iloc[idato]  = datos_radiales['nitrato'].iloc[idato]
    try:
        datos_radiales['ton'].iloc[idato] = datos_radiales['ton'].iloc[idato]  + datos_radiales['nitrito'].iloc[idato] 
    except:
        pass
    try:
        datos_radiales['ton'].iloc[idato] = datos_radiales['ton'].iloc[idato]  + datos_radiales['amonio'].iloc[idato]         
    except:
        pass
    
    # aprovecho para cambiar el nombre de la estación
    if datos_radiales['ID_estacion'].iloc[idato] == 'E2CO':
        datos_radiales['ID_estacion'].iloc[idato]  = '2'    
    if datos_radiales['ID_estacion'].iloc[idato] == 'E4CO':
        datos_radiales['ID_estacion'].iloc[idato]  = '4'    
    if datos_radiales['ID_estacion'].iloc[idato] == 'E3CO':
        datos_radiales['ID_estacion'].iloc[idato]  = '3' 
    if datos_radiales['ID_estacion'].iloc[idato] == 'E3ACO':
        datos_radiales['ID_estacion'].iloc[idato]  = '3A' 
    if datos_radiales['ID_estacion'].iloc[idato] == 'E3BCO':
        datos_radiales['ID_estacion'].iloc[idato]  = '3B' 
    if datos_radiales['ID_estacion'].iloc[idato] == 'E3CCO':
        datos_radiales['ID_estacion'].iloc[idato]  = '3C'         

datos_radiales = datos_radiales.rename(columns={"Fecha": "fecha_muestreo", "Prof":"presion_ctd", "t":"temperatura_ctd","S":"salinidad_ctd","E":"par_ctd", 
                                                "O2 umol/kg":"oxigeno_wk","Cla":"clorofila_a","ID_estacion":"estacion"})  

# Recupera el identificador del programa de muestreo
id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)

# # Encuentra la estación asociada a cada registro
# print('Asignando la estación correspondiente a cada medida')
# datos_radiales = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)


# # Encuentra las salidas al mar correspondientes
# print('Asignando la salida correspondiente a cada medida')
# tipo_salida    = 'MENSUAL'   
# datos_radiales = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
 
# # Encuentra el identificador asociado a cada registro
# print('Asignando el registro correspondiente a cada medida')
# datos_radiales = FUNCIONES_PROCESADO.evalua_registros(datos_radiales,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
   
# # # # # # Introduce los datos en la base de datos
# print('Introduciendo los datos en la base de datos')
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
# FUNCIONES_PROCESADO.inserta_datos(datos_radiales,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)



datos = datos_radiales
nombre_programa = programa_muestreo
from sqlalchemy import create_engine
import psycopg2
itipo_informacion = 2

# Busca de cuántos años diferentes contiene información el dataframe
vector_auxiliar_tiempo = numpy.zeros(datos.shape[0],dtype=int)
for idato in range(datos.shape[0]):
    vector_auxiliar_tiempo[idato] = datos['fecha_muestreo'][idato].year
anhos_muestreados                 = numpy.unique(vector_auxiliar_tiempo)
datos['año']                      = vector_auxiliar_tiempo 

# Procesado para cada uno de los años incluidos en el dataframe importado
for ianho in range(len(anhos_muestreados)):
    
    anho_procesado = anhos_muestreados[ianho]
           
    if itipo_informacion == 1 or itipo_informacion == 2:
        # Selecciona la información de cada uno de los años 
        fechas_anuales  = datos['fecha_muestreo'][datos['año']==anhos_muestreados[ianho]]
    
        # Encuentra la fecha de final de muestreo 
        fecha_actualizacion = fechas_anuales.max()

    # Recupera los datos disponibles        
    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    datos_bd         = pandas.read_sql('SELECT * FROM estado_procesos', conn_psql)
    conn_psql.dispose()
            
    datos_bd_programa       = datos_bd[datos_bd['programa']==id_programa]
    datos_bd_programa_anho  = datos_bd_programa[datos_bd_programa['año']==anho_procesado]
    
    if datos_bd_programa_anho.shape[0] == 0:
        id_proceso = datos_bd.shape[0] + 1
    else:
        id_proceso = datos_bd_programa_anho['id_proceso'].iloc[0] 
        
    conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()  
    if itipo_informacion == 1:
        instruccion_sql = "INSERT INTO estado_procesos (id_proceso,programa,nombre_programa,año,fecha_final_muestreo,contacto_muestreo) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id_proceso) DO UPDATE SET (programa,nombre_programa,año,fecha_final_muestreo,contacto_muestreo) = ROW(EXCLUDED.programa,EXCLUDED.nombre_programa,EXCLUDED.año,EXCLUDED.fecha_final_muestreo,EXCLUDED.contacto_muestreo);"   
    if itipo_informacion == 2:
        instruccion_sql = "INSERT INTO estado_procesos (id_proceso,programa,nombre_programa,año,fecha_analisis_laboratorio,contacto_analisis_laboratorio) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id_proceso) DO UPDATE SET (programa,nombre_programa,año,fecha_analisis_laboratorio,contacto_analisis_laboratorio) = ROW(EXCLUDED.programa,EXCLUDED.nombre_programa,EXCLUDED.año,EXCLUDED.fecha_analisis_laboratorio,EXCLUDED.contacto_analisis_laboratorio);"   
    if itipo_informacion == 3:
        instruccion_sql = "INSERT INTO estado_procesos (id_proceso,programa,nombre_programa,año,fecha_post_procesado,contacto_post_procesado) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id_proceso) DO UPDATE SET (programa,nombre_programa,año,fecha_post_procesado,contacto_post_procesado) = ROW(EXCLUDED.programa,EXCLUDED.nombre_programa,EXCLUDED.año,EXCLUDED.fecha_post_procesado,EXCLUDED.contacto_post_procesado);"   
                    
        
    cursor.execute(instruccion_sql, (int(id_proceso),int(id_programa),nombre_programa,int(anho_procesado),fecha_actualizacion,email_contacto))
    conn.commit() 







# # # Actualiza estado
# print('Actualizando el estado de los procesos')
# FUNCIONES_AUXILIARES.actualiza_estado(datos_radiales,id_programa,programa_muestreo,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto)

