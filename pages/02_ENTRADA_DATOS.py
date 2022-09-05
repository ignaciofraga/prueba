# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import streamlit as st
import psycopg2
import pandas 
from io import BytesIO
import pandas.io.sql as psql
from sqlalchemy import create_engine

from pages.COMUNES import FUNCIONES_INSERCION

# DATOS BASE

logo_IEO_reducido            =  'DATOS/IMAGENES/ieo.ico'
archivo_plantilla            =  'DATOS/PLANTILLA.xlsx'
archivo_instrucciones        =  'DATOS/INSTRUCCIONES_PLANTILLA.zip'
archivo_variables_base_datos =  'DATOS/VARIABLES.xlsx'
min_dist          = 50 # minima distancia para considerar dos estaciones diferentes

##### FUNCIONES AUXILIARES ######

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])




##### WEB STREAMLIT #####

# Encabezados y titulos 
st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
st.title('Servicio de entrada de datos en la base de datos del C.O de A Coruña')

# Recupera la tabla de los programas disponibles en la base de datos, como un dataframe
conn = init_connection()
df_programas = psql.read_sql('SELECT * FROM programas', conn)
conn.close()

# Listado del tipo de dato a introducir      
listado_opciones = ['Análisis de laboratorio','Procesado o revisión de datos ya disponibles']

# Selección de programa, tipo de dato y correo de contacto
programa_elegido  = st.selectbox('Selecciona el programa al que corresponden los datos a insertar',(df_programas['nombre_programa']))
tipo_dato_elegido = st.selectbox('Selecciona el origen de los datos a insertar', (listado_opciones))
email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")


### Recupera los identificadores de la selección hecha

# Recupera el identificador del programa seleccionado
id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

# Encuentra el identificador asociado al tipo de dato
for iorigen in range(len(listado_opciones)):
    if listado_opciones[iorigen] == tipo_dato_elegido:
        id_opcion_elegida = iorigen
        
### Subida de archivos

# Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
direccion_host = st.secrets["postgres"].host
base_datos     = st.secrets["postgres"].dbname
usuario        = st.secrets["postgres"].user
contrasena     = st.secrets["postgres"].password
puerto         = st.secrets["postgres"].port

# Boton para subir los archivos de datos
listado_archivos_subidos = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
for archivo_subido in listado_archivos_subidos:

    ## Lectura de los datos subidos
    # Programa PELACUS   
    if id_programa_elegido == 1: 
        try:
            datos       = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_subido)
            texto_exito = 'Lectura del archivo ' + archivo_subido.name + ' realizada correctamente'
            st.success(texto_exito)
        except:
            texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")

    # Programa Radiales (2-Vigo, 3-Coruña, 4-Santander)    
    if id_programa_elegido == 2 or id_programa_elegido == 3 or id_programa_elegido == 4: 
        try:    
            datos = FUNCIONES_INSERCION.lectura_datos_radiales(archivo_subido,direccion_host,base_datos,usuario,contrasena,puerto)
            texto_exito = 'Lectura del archivo ' + archivo_subido.name + ' realizada correctamente'
            st.success(texto_exito)
        except:
            texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")
    
    ## Realiza un control de calidad primario a los datos importados   
    try:
        datos_corregidos = FUNCIONES_INSERCION.control_calidad(datos,archivo_variables_base_datos) 
        texto_exito = 'Control de calidad de los datos del archivo ' + archivo_subido.name + ' realizado correctamente'
        st.success(texto_exito)
    except:
        texto_error = 'Error en el control de calidad de los datos del archivo ' + archivo_subido.name
        st.warning(texto_error, icon="⚠️")


    # ## Introduce los datos en la base de datos
    # try:
    #     with st.spinner('Insertando datos en la base de datos'):
    #         FUNCIONES_INSERCION.inserta_datos(datos_corregidos,min_dist,programa_elegido,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)
    #     texto_exito = 'Datos del archivo ' + archivo_subido.name + ' insertados en la base de datos correctamente'
    #     st.success(texto_exito)
        
    # except:
    #     texto_error = 'Error al insertar los datos importados en la base de datos'
    #     st.warning(texto_error, icon="⚠️")        
        
    # # Actualiza estado
    # try:
    #     FUNCIONES_INSERCION.actualiza_estado(datos_corregidos,id_programa_elegido,programa_elegido,tipo_dato_elegido,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
    #     texto_exito = 'Fechas de procesado de la información contenidas en la base de datos actualizadas correctamente'
    #     st.success(texto_exito)    
    # except:
    #     texto_error = 'Error al actualizar las fechas de procesado en la base de datos'
    #     st.warning(texto_error, icon="⚠️")    
            

    import numpy 
    from pyproj import Proj
    import math
    import datetime
    id_programa = id_programa_elegido
    nombre_programa = programa_elegido
 
    texto ='init' + (datetime.datetime.now()).strftime('%H:%M:%S')
    st.text(texto)   
 
    # recupera las estaciones disponibles en la base de datos
    conn = init_connection()
    datos_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
    conn.close() 

    # identifica la estación asociada a cada registro
    datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    proy_datos                = Proj(proj='utm',zone=29,ellps='WGS84', preserve_units=False) # Referencia coords
    
    for iregistro in range(datos.shape[0]): 
        if datos_estaciones.shape[0] == 0:
            datos['id_estacion_temp'][iregistro] = 1
            datos_estaciones['nombre'] = datos['estacion']
            datos_estaciones['nombre'] = datos['estacion']           
            
            nueva_estacion = {'id_estacion':1,'nombre_estacion':datos['estacion'][iregistro], 'latitud':datos['latitud'][iregistro], 'longitud':datos['longitud'][iregistro], 'programa':id_programa}
            datos_estaciones = datos_estaciones.concat(nueva_estacion, ignore_index=True)

        else:
           vector_distancias      = numpy.zeros(datos_estaciones.shape[0])
           vector_identificadores = numpy.zeros(datos_estaciones.shape[0],dtype=int)
           
           # Determina la distancia de cada registro a las estaciones incluidas en la base de datos
           for iestacion_disponible in range(datos_estaciones.shape[0]):
               x_muestreo, y_muestreo = proy_datos(datos['longitud'][iregistro], datos['latitud'][iregistro], inverse=False)
               x_bd, y_bd             = proy_datos(datos_estaciones['longitud'][iestacion_disponible], datos_estaciones['latitud'][iestacion_disponible], inverse=False)
               distancia              = math.sqrt((((x_muestreo-x_bd)**2) + ((y_muestreo-y_bd)**2)))
               
               vector_distancias[iestacion_disponible]      = distancia
               vector_identificadores[iestacion_disponible] = int(datos_estaciones['id_estacion'][iestacion_disponible])
               
           # Si la distancia a alguna de las estaciones es menor a la distancia mínima, la estación ya está en la base de datos
           if min(vector_distancias) <= min_dist :
               ipos_minimo = numpy.argmin(vector_distancias)
               datos['id_estacion_temp'][iregistro] = vector_identificadores[ipos_minimo]
               
           # En caso contrario, la estación es nueva y se añade a la base de datos
           else:
               indice_insercion = int(max(datos_estaciones['id_estacion']) + 1)
 
               nueva_estacion = {'id_estacion':indice_insercion,'nombre_estacion':datos['estacion'][iregistro], 'latitud':datos['latitud'][iregistro], 'longitud':datos['longitud'][iregistro], 'programa':id_programa}
               datos_estaciones = datos_estaciones.concat(nueva_estacion, ignore_index=True)

             
               datos['id_estacion_temp'][iregistro] = indice_insercion 


    con_engine = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    engine = create_engine(con_engine)
    st.text(con_engine)
#   engine = create_engine('postgresql://postgres:m0nt34lt0@193.146.155.99:5432/COAC')
    datos_estaciones.to_sql('estaciones', engine, if_exists='replace')
    
    # conn = init_connection()           
    # datos_estaciones.to_sql('estaciones', con=conn, if_exists='replace', index=False)         
    # conn.close()  
    
    texto ='estaciones 1' + (datetime.datetime.now()).strftime('%H:%M:%S')
    st.text(texto)   
 
    
    ### IDENTIFICA LAS ESTACIONES MUESTREADAS Y EVALUA SI YA EXISTEN EN LA BASE DE DATOS (TABLA ESTACIONES)
    
    datos['id_estacion_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
    
    proy_datos                = Proj(proj='utm',zone=29,ellps='WGS84', preserve_units=False) # Referencia coords
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    for iregistro in range(datos.shape[0]):  
    
        # Busca el identificador de la estacion
        instruccion_sql = 'SELECT id_estacion,longitud,latitud FROM estaciones WHERE programa = %s ;'
        cursor.execute(instruccion_sql,(str(id_programa)))
        estaciones_disponibles =cursor.fetchall()
        conn.commit()     
        
        # Si no hay registros de estaciones en la bas de datos, insertar la estación muestreada directamente
        if len(estaciones_disponibles) == 0:
            datos_insercion = (str(datos['estacion'][iregistro]),round(datos['latitud'][iregistro],4),round(datos['longitud'][iregistro],4),int(id_programa))
            instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
            cursor.execute(instruccion_sql, (datos_insercion))
            conn.commit() 
    
            datos['id_estacion_temp'][iregistro] = 1
        
        # En caso contrario, buscar si hay una estación en el mismo punto (+- min_dist)
        else:
            
            vector_distancias      = numpy.zeros(len(estaciones_disponibles))
            vector_identificadores = numpy.zeros(len(estaciones_disponibles),dtype=int)
            
            # Determina la distancia de cada registro a las estaciones incluidas en la base de datos
            for iestacion_disponible in range(len(estaciones_disponibles)):
                x_muestreo, y_muestreo = proy_datos(datos['longitud'][iregistro], datos['latitud'][iregistro], inverse=False)
                x_bd, y_bd             = proy_datos(float(estaciones_disponibles[iestacion_disponible][1]), float(float(estaciones_disponibles[iestacion_disponible][2])), inverse=False)
                distancia              = math.sqrt((((x_muestreo-x_bd)**2) + ((y_muestreo-y_bd)**2)))
                
                vector_distancias[iestacion_disponible]      = distancia
                vector_identificadores[iestacion_disponible] = int(estaciones_disponibles[iestacion_disponible][0])
                
            # Si la distancia a alguna de las estaciones es menor a la distancia mínima, la estación ya está en la base de datos
            if min(vector_distancias) <= min_dist :
                ipos_minimo = numpy.argmin(vector_distancias)
                datos['id_estacion_temp'][iregistro] = int(estaciones_disponibles[ipos_minimo][0])
                
            # En caso contrario, la estación es nueva y se añade a la base de datos
            else:
                indice_insercion = max(vector_identificadores) + 1
                datos_insercion = (str(datos['estacion'][iregistro]),round(datos['latitud'][iregistro],4),round(datos['longitud'][iregistro],4),int(id_programa))
                instruccion_sql = "INSERT INTO estaciones (nombre_estacion,latitud,longitud,programa) VALUES (%s,%s,%s,%s) ON CONFLICT (id_estacion) DO NOTHING;"   
                cursor.execute(instruccion_sql, (datos_insercion))
                conn.commit() 
        
                datos['id_estacion_temp'][iregistro] = indice_insercion
                
    cursor.close()
    conn.close()
    
    texto ='estaciones 2' + (datetime.datetime.now()).strftime('%H:%M:%S')
    st.text(texto)
     
    ### DETERMINA EL NUMERO DE REGISTRO DE CADA MUESTREO 
    
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    cursor = conn.cursor()
    
    datos['id_muestreo_temp'] = numpy.zeros(datos.shape[0],dtype=int) 
     
    
    for idato in range(datos.shape[0]):
    #for idato in range(300): 
    
        nombre_muestreo = nombre_programa + '_' + str(datos['fecha_muestreo'][idato].year) + '_E' + str(datos['id_estacion_temp'][idato])
    
        # Por seguridad, convierte el identificador de botella a entero si éste existe
        if datos['botella'][idato] is not None:
            id_botella = int(datos['botella'][idato])
        else:
            id_botella = None
            
        # Por incompatibilidad con POSTGRESQL hay que "desmontar" y volver a montar las fechas
        anho           = datos['fecha_muestreo'][idato].year # 
        mes            = datos['fecha_muestreo'][idato].month
        dia            = datos['fecha_muestreo'][idato].day
        fecha_consulta = datetime.date(anho,mes,dia) 
        
        # Intenta insertar el muestreo correspondiente al registro. Si ya existe en la base de datos no hará nada, de lo contrario añadirá el nuevo muestreo
        # Distinta instrucción según haya información de hora o no (para hacer el script más tolerante a fallos)
        
        if datos['hora_muestreo'][idato] is not None:
            # Si es un string conviertelo a time
            if isinstance(datos['hora_muestreo'][idato], str) is True:
                hora_temporal = datetime.datetime.strptime(datos['hora_muestreo'][idato],'%H:%M')
            # Si es un datetime conviertelo a time
            elif isinstance(datos['hora_muestreo'][idato], datetime.datetime) is True:
                hora_temporal = datos['hora_muestreo'][idato].time()
            else:
                hora_temporal= datos['hora_muestreo'][idato]
            
            # Por incompatibilidad con POSTGRESQL también hay que "desmontar" y volver a montar las horas
            hora          = hora_temporal.hour
            minuto        = hora_temporal.minute
            hora_consulta = datetime.time(hora,minuto) 
                
            instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,hora_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
            cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,hora_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            conn.commit()
     
            instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND hora_muestreo=%s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
            cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,datos['hora_muestreo'][idato],round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            id_muestreos_bd =cursor.fetchone()
            conn.commit()     
        
    
        else:
            
            instruccion_sql = "INSERT INTO muestreos_discretos (nombre_muestreo,estacion,fecha_muestreo,profundidad,botella,configuracion_perfilador,configuracion_superficie) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_muestreo,profundidad,configuracion_perfilador,configuracion_superficie) DO NOTHING;"   
            cursor.execute(instruccion_sql, (nombre_muestreo,int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),id_botella,int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            conn.commit()
                
            instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos WHERE estacion = %s AND fecha_muestreo = %s AND profundidad = %s AND configuracion_perfilador = %s AND configuracion_superficie = %s;"
            cursor.execute(instruccion_sql, (int(datos['id_estacion_temp'][idato]),fecha_consulta,round(datos['profundidad'][idato],2),int(datos['configuracion_perfilador'][idato]),int(datos['configuracion_superficie'][idato])))
            id_muestreos_bd =cursor.fetchone()
            conn.commit() 
        
        datos['id_muestreo_temp'][idato] =  id_muestreos_bd[0]
    
    cursor.close()
    conn.close()     

    
    texto ='registros' + (datetime.datetime.now()).strftime('%H:%M:%S')
    st.text(texto)
   
  
    
  
    
  
    
  
    
  
    
### Despliega un formulario para elegir el programa y el tipo de información a insertar

# # Listado del tipo de dato a introducir      
# listado_opciones = ['Análisis de laboratorio','Procesado o revisión de datos ya disponibles']

# with st.form("Formulario insercion"):
#     st.write("Selecciona el origen y tipo de los datos a insertar")
#     programa_elegido  = st.selectbox('Selecciona el programa al que corresponden los datos a insertar',(df_programas['nombre_programa']))
#     tipo_dato_elegido = st.selectbox('Selecciona el origen de los datos a insertar', (listado_opciones))
#     email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")

#     # Botón de envío para confirmar selección
#     submitted = st.form_submit_button("Enviar")
#     if submitted:
#         st.write("Resultado de la selección. Programa: ", programa_elegido, ". Tipo de dato: ", tipo_dato_elegido)
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  # ### Recupera los identificadores de la selección hecha

  # # Recupera el identificador del programa seleccionado
  # id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

  # # Encuentra el identificador asociado al tipo de dato
  # for iorigen in range(len(listado_opciones)):
  #     if listado_opciones[iorigen] == tipo_dato_elegido:
  #         id_opcion_elegida = iorigen
          
  #     ### Subida de archivos
      
  #     # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
  #     direccion_host = st.secrets["postgres"].host
  #     base_datos     = st.secrets["postgres"].dbname
  #     usuario        = st.secrets["postgres"].user
  #     contrasena     = st.secrets["postgres"].password
  #     puerto         = st.secrets["postgres"].port
      
  #     # Boton para subir los archivos de datos
  #     listado_archivos_subidos = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
  #     for archivo_subido in listado_archivos_subidos:
  #         st.write("Archivo subido:", archivo_subido.name)
      
  #         ## Lectura de los datos subidos
       
  #         # Programa PELACUS   
  #         if id_programa_elegido == 1: 
              
  #             try:
      
  #                 datos       = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_subido)
  #                 texto_exito = 'Lectura del archivo ' + archivo_subido.name + ' realizada correctamente'
  #                 st.success(texto_exito)
  #             except:
  #                 texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
  #                 st.warning(texto_error, icon="⚠️")
      
  #         # Programa Radiales (2-Vigo, 3-Coruña, 4-Santander)    
  #         if id_programa_elegido == 2 or id_programa_elegido == 3 or id_programa_elegido == 4: 
          
  #             try:    
  #                 datos = FUNCIONES_INSERCION.lectura_datos_radiales(archivo_subido,direccion_host,base_datos,usuario,contrasena,puerto)
  #                 texto_exito = 'Lectura del archivo ' + archivo_subido.name + ' realizada correctamente'
  #                 st.success(texto_exito)
  #             except:
  #                 texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
  #                 st.warning(texto_error, icon="⚠️")
          
  #         ## Realiza un control de calidad primario a los datos importados   
  #         try:
  #             datos_corregidos = FUNCIONES_INSERCION.control_calidad(datos,archivo_variables_base_datos) 
  #             texto_exito = 'Control de calidad de los datos del archivo ' + archivo_subido.name + ' realizado correctamente'
  #             st.success(texto_exito)
  #         except:
  #             texto_error = 'Error en el control de calidad de los datos del archivo ' + archivo_subido.name
  #             st.warning(texto_error, icon="⚠️")
      
      
  #         ## Introduce los datos en la base de datos
  #         try:
  #             with st.spinner('Insertando datos en la base de datos'):
  #                 FUNCIONES_INSERCION.inserta_datos(datos_corregidos,min_dist,programa_elegido,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)
  #             texto_exito = 'Datos del archivo ' + archivo_subido.name + ' insertados en la base de datos correctamente'
  #             st.success(texto_exito)
              
  #         except:
  #             texto_error = 'Error al insertar los datos importados en la base de datos'
  #             st.warning(texto_error, icon="⚠️")        
              
  #         # Actualiza estado
  #         try:
  #             FUNCIONES_INSERCION.actualiza_estado(datos_corregidos,id_programa_elegido,programa_elegido,tipo_dato_elegido,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
  #             texto_exito = 'Fechas de procesado de la información contenidas en la base de datos actualizadas correctamente'
  #             st.success(texto_exito)    
  #         except:
  #             texto_error = 'Error al actualizar las fechas de procesado en la base de datos'
  #             st.warning(texto_error, icon="⚠️")    
          











# ### Recordatorio de formato

# # Despliega un recordatorio de ajustar los datos a un formato y un botón para descargar una plantilla
# st.write('')
# st.warning('Los archivos a subir deben ajustarse a la plantilla disponible más abajo', icon="⚠️")
# st.write('')


   
# ## Botón para descargar la plantilla
# datos_plantilla = pandas.read_excel(archivo_plantilla, 'DATOS')

# output = BytesIO()
# writer = pandas.ExcelWriter(output, engine='xlsxwriter')
# datos_plantilla.to_excel(writer, index=False, sheet_name='DATOS')
# workbook = writer.book
# worksheet = writer.sheets['DATOS']
# writer.save()
# datos_exporta = output.getvalue()

# st.download_button(
#     label="DESCARGAR PLANTILLA",
#     data=datos_exporta,
#     file_name='Plantilla_datos.xlsx',
#     help= 'Descarga un archivo .xlsx de referencia para subir los datos solicitados',
#     mime="application/vnd.ms-excel"
# )


# with open(archivo_instrucciones, "rb") as fp:
#     btn = st.download_button(
#         label="DESCARGA INTRUCCIONES",
#         data=fp,
#         file_name="Instrucciones.zip",
#         help= 'Descarga un archivo con instrucciones para rellenar la plantilla de datos',
#         mime="application/zip"
#     )    


