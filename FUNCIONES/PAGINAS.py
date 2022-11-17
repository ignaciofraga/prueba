# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 08:05:55 2022

@author: ifraga
"""
import streamlit as st
import pandas.io.sql as psql
import datetime
import pandas 
import matplotlib.pyplot as plt
import st_aggrid 
from io import BytesIO
import numpy
import psycopg2
from PIL import Image
from dateutil.relativedelta import relativedelta
import matplotlib.dates as mdates
import json
import time
#from random import randint

#import FUNCIONES_INSERCION
from FUNCIONES import FUNCIONES_INSERCION
from FUNCIONES.FUNCIONES_AUXILIARES import init_connection
from FUNCIONES.FUNCIONES_AUXILIARES import estado_procesos

# from pages.COMUNES import FUNCIONES_AUXILIARES

pandas.options.mode.chained_assignment = None


###############################################################################
####################### PÁGINA PRINCIPAL ######################################
###############################################################################


def principal():

    logo_IEO_principal    = 'DATOS/IMAGENES/logo-CSIC.jpg'    

    st.title("Servicio de información de nutrientes del C.O de A Coruña")

    # Añade el logo del IEO
    imagen_pagina = Image.open(logo_IEO_principal) 
    st.image(imagen_pagina)





###############################################################################
################## PÁGINA DE ENTRADA DE DATOS DE NUTRIENTES ###################
###############################################################################


def entrada_datos():
    
    archivo_plantilla     = 'DATOS/PLANTILLA.xlsx'
    archivo_instrucciones = 'DATOS/PLANTILLA.zip'
    
    # Encabezados y titulos 
    #st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
    st.title('Servicio de entrada de datos del C.O de A Coruña')

    # Recupera la tabla de los programas disponibles en la base de datos, como un dataframe
    conn = init_connection()
    df_programas = psql.read_sql('SELECT * FROM programas', conn)
    conn.close()


    # Despliega un formulario para elegir el programa y la fecha a consultar
    with st.form("Formulario seleccion"):

        listado_opciones = ['Datos de NUTRIENTES procedentes de análisis de laboratorio','Datos de NUTRIENTES procesados o revisados','Datos de MUESTREOS ']
        tipo_dato_elegido = st.selectbox('Selecciona el tipo de información que se va a subir', (listado_opciones))
        
        col1, col2 = st.columns(2,gap="small")
        #nombre_programa, tiempo_consulta = st.columns((1, 1))
        with col1:
            programa_elegido  = st.selectbox('Selecciona el programa al que corresponde la información',(df_programas['nombre_programa']))
        with col2:
            email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")

        # Botón de envío para confirmar selección
        st.form_submit_button("Enviar")



    ### Recupera los identificadores de la selección hecha

    # Recupera el identificador del programa seleccionado
    id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

    # Encuentra el identificador asociado al tipo de dato
    for iorigen in range(len(listado_opciones)):
        if listado_opciones[iorigen] == tipo_dato_elegido:
            id_opcion_elegida = iorigen + 1
                    
    # Si se elige introducir un estadillo, recordar que se ajusten a la plantilla
    if id_opcion_elegida ==3:
        texto_error = 'IMPORTANTE. Los datos a subir deben ajustarse a la plantilla facilitada' 
        st.warning(texto_error, icon="⚠️")

    #    st.download_button('DESCARGAR PLANTILLA E INSTRUCCIONES', archivo_instrucciones, file_name='PLANTILLA.zip')        

        with open(archivo_instrucciones, "rb") as fp:
            st.download_button(
                label="DESCARGAR PLANTILLA E INSTRUCCIONES",
                data=fp,
                file_name="PLANTILLA.zip",
                mime="application/zip"
            )
            
        
    fecha_actualizacion = datetime.date.today()    
        
    ### Subida de archivos

    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port

    col1 = st.columns(1)

    # Boton para subir los archivos de datos
    listado_archivos_subidos = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
    for archivo_subido in listado_archivos_subidos:

        
        # Opciones 1 y 2, lectura de datos de nutrientes
        if id_opcion_elegida == 1 or id_opcion_elegida == 2:
            ## Lectura de los datos subidos 
            # Programa PELACUS   
            if id_programa_elegido == 1: 
                try:
                    datos       = FUNCIONES_INSERCION.lectura_datos_pelacus(archivo_subido)
                    texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
                    st.success(texto_exito)
                except:
                    texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
                    st.warning(texto_error, icon="⚠️")
        
            # Programa Radiales (2-Vigo, 3-Coruña, 4-Santander)    
            if id_programa_elegido == 2 or id_programa_elegido == 3 or id_programa_elegido == 4: 
                try:    
                    datos = FUNCIONES_INSERCION.lectura_datos_radiales(archivo_subido,direccion_host,base_datos,usuario,contrasena,puerto)
                    texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
                    st.success(texto_exito)
                except:
                    texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
                    st.warning(texto_error, icon="⚠️")
            
        # Opcion 3, lectura de estadillo con datos de entrada
        if id_opcion_elegida == 3:
            try:
                datos,texto_error = FUNCIONES_INSERCION.lectura_datos_estadillo(archivo_subido,archivo_plantilla)
                texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
                st.success(texto_exito)
                if len(texto_error)>0:
                    for iaviso in range(len(texto_error)):
                        st.warning(texto_error[iaviso], icon="⚠️")
            
            except:
                texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
                st.warning(texto_error, icon="⚠️")


        # Realiza un control de calidad primario a los datos importados   
        try:
            datos,textos_aviso = FUNCIONES_INSERCION.control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto) 
            texto_exito = 'Control de calidad de los datos del archivo ' + archivo_subido.name + ' realizado correctamente'
            st.success(texto_exito)
            if len(textos_aviso)>0:
                for iaviso in range(len(textos_aviso)):
                    st.warning(textos_aviso[iaviso], icon="⚠️")
            
        except:
            texto_error = 'Error en el control de calidad de los datos del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")

        # Introduce los datos en la base de datos
        # try:
     
        with st.spinner('Insertando datos en la base de datos'):

            datos = FUNCIONES_INSERCION.evalua_estaciones(datos,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)  

            datos = FUNCIONES_INSERCION.evalua_registros(datos,programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)

            FUNCIONES_INSERCION.inserta_datos_fisica(datos,direccion_host,base_datos,usuario,contrasena,puerto)

            FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos,direccion_host,base_datos,usuario,contrasena,puerto)
            
        texto_exito = 'Datos del archivo ' + archivo_subido.name + ' insertados en la base de datos correctamente'
        st.success(texto_exito)

        # except:
        #     texto_error = 'Error en la subida de los datos del archivo ' + archivo_subido.name
        #     st.warning(texto_error, icon="⚠️")
            
        # Actualiza estado
        try:
            
            FUNCIONES_INSERCION.actualiza_estado(datos,fecha_actualizacion,id_programa_elegido,programa_elegido,id_opcion_elegida,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
         
            texto_exito = 'Las fechas de procesado contenidas en la base de datos han sido actualizadas correctamente'
            st.success(texto_exito)    
        except:
            texto_error = 'Error al actualizar las fechas de procesado en la base de datos'
            st.warning(texto_error, icon="⚠️")    
     
      

        





###############################################################################
################## PÁGINA DE ENTRADA DE ESTADILLOS DE DATOS ###################
###############################################################################
        
def entrada_estadillos():
    
    archivo_plantilla     = 'DATOS/PLANTILLA.xlsx'
    archivo_instrucciones = 'DATOS/PLANTILLA.zip'
    
    # Encabezados y titulos 
    #st.set_page_config(page_title='ENTRADA DE DATOS', layout="wide",page_icon=logo_IEO_reducido) 
    st.title('Servicio de entrada de datos del C.O de A Coruña')

    # Recupera la tabla de los programas disponibles en la base de datos, como un dataframe
    conn = init_connection()
    df_programas = psql.read_sql('SELECT * FROM programas', conn)
    conn.close()


    # Despliega un formulario para elegir el programa y la fecha a consultar
    with st.form("Formulario seleccion"):

        col1, col2 = st.columns(2,gap="small")
        #nombre_programa, tiempo_consulta = st.columns((1, 1))
        with col1:
            programa_elegido  = st.selectbox('Selecciona el programa al que corresponde la información',(df_programas['nombre_programa']))
        with col2:
            email_contacto    = st.text_input('Correo de contacto', "...@ieo.csic.es")

        # Botón de envío para confirmar selección
        st.form_submit_button("Enviar")



    ### Recupera los identificadores de la selección hecha

    # Recupera el identificador del programa seleccionado
    id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_elegido].values[0])

                    
    # Recordar que se ajusten a la plantilla y facilitar la misma
    texto_error = 'IMPORTANTE. Los datos a subir deben ajustarse a la plantilla facilitada' 
    st.warning(texto_error, icon="⚠️")

    with open(archivo_instrucciones, "rb") as fp:
        st.download_button(
            label="DESCARGAR PLANTILLA E INSTRUCCIONES",
            data=fp,
            file_name="PLANTILLA.zip",
            mime="application/zip"
        )
            
        
    fecha_actualizacion = datetime.date.today()    
        
    ### Subida de archivos

    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port

    col1 = st.columns(1)

    # Boton para subir los archivos de datos
    listado_archivos_subidos = st.file_uploader("Arrastra los archivos a insertar en la base de datos del COAC", accept_multiple_files=True)
    for archivo_subido in listado_archivos_subidos:

        # Lectura del estadillo con datos de entrada
        try:
            datos,texto_error = FUNCIONES_INSERCION.lectura_datos_estadillo(archivo_subido,archivo_plantilla)
            texto_exito = 'Archivo ' + archivo_subido.name + ' leído correctamente'
            st.success(texto_exito)
            if len(texto_error)>0:
                for iaviso in range(len(texto_error)):
                    st.warning(texto_error[iaviso], icon="⚠️")
        
        except:
            texto_error = 'Error en la lectura del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")


        # Realiza un control de calidad primario a los datos importados   
        try:
            datos,textos_aviso = FUNCIONES_INSERCION.control_calidad(datos,direccion_host,base_datos,usuario,contrasena,puerto) 
            texto_exito = 'Control de calidad de los datos del archivo ' + archivo_subido.name + ' realizado correctamente'
            st.success(texto_exito)
            if len(textos_aviso)>0:
                for iaviso in range(len(textos_aviso)):
                    st.warning(textos_aviso[iaviso], icon="⚠️")
            
        except:
            texto_error = 'Error en el control de calidad de los datos del archivo ' + archivo_subido.name
            st.warning(texto_error, icon="⚠️")

        # Introduce los datos en la base de datos
        # try:
     
        with st.spinner('Insertando datos en la base de datos'):

            datos = FUNCIONES_INSERCION.evalua_estaciones(datos,id_programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)  

            datos = FUNCIONES_INSERCION.evalua_registros(datos,programa_elegido,direccion_host,base_datos,usuario,contrasena,puerto)

            FUNCIONES_INSERCION.inserta_datos_fisica(datos,direccion_host,base_datos,usuario,contrasena,puerto)

            FUNCIONES_INSERCION.inserta_datos_biogeoquimica(datos,direccion_host,base_datos,usuario,contrasena,puerto)
            
        texto_exito = 'Datos del archivo ' + archivo_subido.name + ' insertados en la base de datos correctamente'
        st.success(texto_exito)


        try:
            id_opcion_elegida = 3
            FUNCIONES_INSERCION.actualiza_estado(datos,fecha_actualizacion,id_programa_elegido,programa_elegido,id_opcion_elegida,email_contacto,direccion_host,base_datos,usuario,contrasena,puerto)
         
            texto_exito = 'Las fechas de procesado contenidas en la base de datos han sido actualizadas correctamente'
            st.success(texto_exito)    
        except:
            texto_error = 'Error al actualizar las fechas de procesado en la base de datos'
            st.warning(texto_error, icon="⚠️")    
          
        
     
        
     
        
     
        
     
        
     
        
     
        
     
        
     
        
     
        
     
        
     
###############################################################################
############### PÁGINA DE CONSULTA DEL ESTADO DE LOS PROCESOS #################
###############################################################################

def consulta_estado():
        
             
    ### Encabezados y titulos 
    #st.set_page_config(page_title='CONSULTA DATOS', layout="wide",page_icon=logo_IEO_reducido) 
    st.title('Servicio de consulta de información disponible del C.O de A Coruña')
    
    
    # Recupera la tabla de los programas disponibles como un dataframe
    conn = init_connection()
    df_programas = psql.read_sql('SELECT * FROM programas', conn)
    conn.close()
    
    # Despliega un formulario para elegir el programa y la fecha a consultar
    with st.form("Formulario seleccion"):
        col1, col2 = st.columns(2,gap="small")
        #nombre_programa, tiempo_consulta = st.columns((1, 1))
        with col1:
            nombre_programa  = st.selectbox('Selecciona el programa del cual se quiere consultar el estado',(df_programas['nombre_programa']))
        with col2:
            tiempo_consulta = st.date_input("Selecciona fecha de consulta",datetime.date.today())
    
        # Botón de envío para confirmar selección
        submit = st.form_submit_button("Enviar")
    
 
    if submit:
    
        # Recupera el identificador del programa seleccionado
        id_programa = int(df_programas['id_programa'][df_programas['nombre_programa']==nombre_programa].values[0])
        
        ### Consulta a la base de datos las fechas de los distintos procesos (muestreo, análisis y post-procesado)
        
        # Recupera la tabla del estado de los procesos como un dataframe
        conn = init_connection()
        temporal_estado_procesos = psql.read_sql('SELECT * FROM estado_procesos', conn)
        conn.close()
        
        # Extrae los datos disponibles del programa consultado 
        estado_procesos_programa = temporal_estado_procesos[temporal_estado_procesos['programa']==id_programa]
        
        # Bucle if para desplegar información únicamente si hay información del programa seleccionado
        if estado_procesos_programa.shape[0] == 0:
            
            st.warning('No se dispone de información acerca del estado del programa de muestreo seleccionado', icon="⚠️")
        
        else:
        
            # Quita del dataframe las columnas con el identificador del programa y el número registro (no interesa mostrarlo en la web)
            estado_procesos_programa = estado_procesos_programa.drop(['id_proceso','programa'], axis = 1)
            
            # Reemplaza los nan por None
            estado_procesos_programa = estado_procesos_programa.fillna(numpy.nan).replace([numpy.nan], [None])
            
            # Actualiza el indice del dataframe 
            indices_dataframe         = numpy.arange(0,estado_procesos_programa.shape[0],1,dtype=int)
            estado_procesos_programa['id_temp'] = indices_dataframe
            estado_procesos_programa.set_index('id_temp',drop=True,append=False,inplace=True)
            
            
            ### Determina el estado de cada proceso, en la fecha seleccionada
            estado_procesos_programa['estado']    = ''
            estado_procesos_programa['contacto']  = ''
            estado_procesos_programa['id_estado'] = 0
            estado_procesos_programa['fecha actualizacion'] = [None]*estado_procesos_programa.shape[0]
            
            nombre_estados  = ['No disponible','Pendiente de análisis','Analizado','Post-Procesado']
            colores_estados = ['#CD5C5C','#F4A460','#87CEEB','#66CDAA','#2E8B57']
            
            for ianho in range(estado_procesos_programa.shape[0]):
            
                # Caso 3. Fecha de consulta posterior al post-procesado.
                if pandas.isnull(estado_procesos_programa['fecha_post_procesado'][ianho]) is False:
                    if tiempo_consulta >= (estado_procesos_programa['fecha_post_procesado'][ianho]):     
                        estado_procesos_programa['id_estado'][ianho] = 3
                        estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho] 
                        estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_post_procesado'][ianho].strftime("%m/%d/%Y")
                else:
                    
                    # Caso 2. Fecha de consulta posterior al análisis de laboratorio pero anterior a realizar el post-procesado.
                    if pandas.isnull(estado_procesos_programa['fecha_analisis_laboratorio'][ianho]) is False:
                        if tiempo_consulta >= (estado_procesos_programa['fecha_analisis_laboratorio'][ianho]):  # estado_procesos_programa['fecha_analisis_laboratorio'][ianho] is not None:     
                            estado_procesos_programa['id_estado'][ianho] = 2
                            estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho] 
                            estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_analisis_laboratorio'][ianho].strftime("%m/%d/%Y")
                        else:
                            if tiempo_consulta >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
                                estado_procesos_programa['id_estado'][ianho] = 1 
                                estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho]
                                estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_entrada_datos'][ianho].strftime("%m/%d/%Y")
                                                    
                    
                    else:
                        # Caso 1. Fecha de consulta posterior a terminar la campaña pero anterior al análisis en laboratorio, o análisis no disponible. 
                        if pandas.isnull(estado_procesos_programa['fecha_entrada_datos'][ianho]) is False:
                            if tiempo_consulta >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
                                estado_procesos_programa['id_estado'][ianho] = 1 
                                estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho]
                                estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_entrada_datos'][ianho].strftime("%m/%d/%Y")
                                
            
                estado_procesos_programa['estado'][ianho] = nombre_estados[estado_procesos_programa['id_estado'][ianho]]
                            
                
                
                
            # Cuenta el numero de veces que se repite cada estado para sacar un gráfico pie-chart
            num_valores = numpy.zeros(len(nombre_estados),dtype=int)
            for ivalor in range(len(nombre_estados)):
                try:
                    num_valores[ivalor] = estado_procesos_programa['id_estado'].value_counts()[ivalor]
                except:
                    pass
            porcentajes = numpy.round((100*(num_valores/numpy.sum(num_valores))),0)
            
            # Construye el gráfico
            cm              = 1/2.54 # pulgadas a cm
            fig, ax1 = plt.subplots(figsize=(8*cm, 8*cm))
            #ax1.pie(num_valores, explode=explode_estados, colors=listado_colores,labels=listado_estados, autopct='%1.1f%%', shadow=True, startangle=90)
            patches, texts= ax1.pie(num_valores, colors=colores_estados,shadow=True, startangle=90,radius=1.2)
            ax1.axis('equal')  # Para representar el pie-chart como un circulo
            
            # Representa y ordena la leyenda
            etiquetas_leyenda = ['{0} - {1:1.0f} %'.format(i,j) for i,j in zip(nombre_estados, porcentajes)]
            plt.legend(patches, etiquetas_leyenda, loc='lower center', bbox_to_anchor=(-0.1, -0.3),fontsize=8)
            
            
            
            
            
            # Genera un subset del dataframe con los años en los que hay datos, entre los que se seleccionará la fecha a descargar
            datos_disponibles = estado_procesos_programa.loc[estado_procesos_programa['id_estado'] >= 2]
            
            # Genera un dataframe con las columnas que se quieran mostrar en la web
            datos_visor = estado_procesos_programa.drop(columns=['nombre_programa','fecha_final_muestreo','fecha_analisis_laboratorio','fecha_post_procesado','id_estado','contacto_muestreo','contacto_post_procesado'])
            datos_visor = datos_visor[['año','estado','fecha actualizacion','contacto']]
            
            datos_visor = datos_visor.sort_values(by=['año'])
            
            cellsytle_jscode = st_aggrid.shared.JsCode(
            """function(params) {
            if (params.value.includes('No disponible'))
            {return {'color': 'black', 'backgroundColor': '#CD5C5C'}}
            if (params.value.includes('Pendiente de análisis'))
            {return {'color': 'black', 'backgroundColor': '#F4A460'}}
            if (params.value.includes('Analizado'))
            {return {'color': 'black', 'backgroundColor': '#87CEEB'}}
            if (params.value.includes('Post-Procesado'))
            {return {'color': 'black', 'backgroundColor': '#66CDAA'}}
            };""")
            
              
            #    if (params.value.includes('Procesado secundario'))
            #    {return {'color': 'black', 'backgroundColor': '#2E8B57'}}
            
            ########################################
            ### Muestra la informacion en la web ###
            ########################################
            
              
            #Division en dos columnas, una para tabla otra para la imagen
            col1, col2 = st.columns(2,gap="medium")
            
            # Representacion de la tabla de estados
            with col1:
                st.header("Listado de datos")
                gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(datos_visor)
                gb.configure_column("estado", cellStyle=cellsytle_jscode)
            
                gridOptions = gb.build()
                
                data = st_aggrid.AgGrid(
                    datos_visor,
                    gridOptions=gridOptions,
                    enable_enterprise_modules=True,
                    allow_unsafe_jscode=True
                    )    
            
            with col2:
                
                # Representa el pie-chart con el estado de los procesos
                buf = BytesIO()
                fig.savefig(buf, format="png",bbox_inches='tight')
                st.image(buf)
        
        
        
        
        
            # Vuelta a la division en una única columna
            st.columns(1,gap="medium")
        
        
        
            # Condicional para realizar el procesado sólo si hay datos disponibles         
            if datos_disponibles.shape[0] == 0:
                
                st.warning('En la fecha consultada no había ningún dato disponible', icon="⚠️")
                
            else:
            
                # Selecciona el año del que se quiere descargar datos
                datos_disponibles = datos_disponibles.sort_values(by=['año'])
                seleccion = st.selectbox('Selecciona el año del cual se quiere descargar los datos (entre los años disponibles)',        
                    datos_disponibles['año'])
            
            
                anho_consulta         = seleccion #☺datos_disponibles['año'][0]
                fecha_inicio_consulta = datetime.date(anho_consulta,1,1)
                fecha_final_consulta  = datetime.date(anho_consulta+1,1,1)
            
                
                # Primero recupera los registros correspondientes al periodo evaluado y al año consultado
                conn = init_connection()
                cursor = conn.cursor()
                instruccion_sql = "SELECT id_muestreo FROM muestreos_discretos INNER JOIN estaciones ON muestreos_discretos.estacion = estaciones.id_estacion WHERE estaciones.programa = %s AND muestreos_discretos.fecha_muestreo >= %s AND muestreos_discretos.fecha_muestreo < %s;"
                cursor.execute(instruccion_sql,(id_programa,fecha_inicio_consulta,fecha_final_consulta))
                #registros_consulta =cursor.fetchall()
                registros_consulta = [r[0] for r in cursor.fetchall()]
                conn.commit()
                cursor.close()
                conn.close()
                
                indices_dataframe         = numpy.arange(0,len(registros_consulta),1,dtype=int)
                
                # A continuacion recupera las tablas de estaciones, muestreos, datos biogeoquimicos y físicos
                
                conn = init_connection()
                
                temporal_estaciones          = psql.read_sql('SELECT * FROM estaciones', conn)
                
                temporal_muestreos           = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
             
                temporal_datos_biogeoquimica = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
                
                temporal_datos_fisica        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
                       
                conn.close()    
                      
                # Compón dataframes con los registros que interesan y elimina el temporal, para reducir la memoria ocupada
                # En cada dataframe hay que re-definir el indice del registro para luego poder juntar los 3 dataframes 
                datos_biogeoquimicos            = temporal_datos_biogeoquimica[temporal_datos_biogeoquimica['muestreo'].isin(registros_consulta)]
                datos_biogeoquimicos['id_temp'] = indices_dataframe
                datos_biogeoquimicos.set_index('id_temp',drop=True,append=False,inplace=True)
                del(temporal_datos_biogeoquimica)
                datos_biogeoquimicos = datos_biogeoquimicos.drop(columns=['id_disc_biogeoquim','muestreo'])
                
                datos_fisicos = temporal_datos_fisica[temporal_datos_fisica['muestreo'].isin(registros_consulta)]
                datos_fisicos['id_temp'] = indices_dataframe
                datos_fisicos.set_index('id_temp',drop=True,append=False,inplace=True) 
                del(temporal_datos_fisica)
                datos_fisicos = datos_fisicos.drop(columns=['id_disc_fisica','muestreo'])
                
                datos_muestreo = temporal_muestreos[temporal_muestreos['id_muestreo'].isin(registros_consulta)]
                datos_muestreo['id_temp'] = indices_dataframe
                datos_muestreo.set_index('id_temp',drop=True,append=False,inplace=True)      
                datos_muestreo = datos_muestreo.drop(columns=['id_muestreo','configuracion_perfilador','configuracion_superficie'])
                datos_muestreo['fecha_muestreo'] = pandas.to_datetime(datos_muestreo['fecha_muestreo']).dt.date
                try:
                    datos_muestreo['hora_muestreo'] = datos_muestreo['hora_muestreo'].apply(lambda x: x.replace(tzinfo=None))   
                except:
                    pass
            
                del(temporal_muestreos)
                
                
                
                # Añade las coordenadas de cada muestreo, a partir de la estación asociada
                datos_muestreo['latitud']  = numpy.zeros(datos_muestreo.shape[0])
                datos_muestreo['longitud'] = numpy.zeros(datos_muestreo.shape[0])    
                datos_muestreo['estacion_temp'] = [None]*datos_muestreo.shape[0]
                for iregistro in range(datos_muestreo.shape[0]):
                    datos_muestreo['latitud'][iregistro] = temporal_estaciones['latitud'][temporal_estaciones['id_estacion']==datos_muestreo['estacion'][iregistro]]
                    datos_muestreo['longitud'][iregistro] = temporal_estaciones['longitud'][temporal_estaciones['id_estacion']==datos_muestreo['estacion'][iregistro]]  
                    
                    aux = temporal_estaciones['id_estacion']==datos_muestreo['estacion'][iregistro]
                    if any(aux) is True:
                        indices_datos = [i for i, x in enumerate(aux) if x]
                        datos_muestreo['estacion_temp'][iregistro]  = temporal_estaciones['nombre_estacion'][indices_datos[0]]
                        
                datos_muestreo = datos_muestreo.drop(columns=['estacion'])
                datos_muestreo = datos_muestreo.rename(columns={"estacion_temp":"estacion"})
                
                
                datos_muestreo = datos_muestreo[['nombre_muestreo','fecha_muestreo','hora_muestreo','estacion','num_cast','latitud','longitud','presion_ctd','botella']]
        
                
                
                del(temporal_estaciones)        
                
                # Une los dataframes resultantes
                datos_compuesto = pandas.concat([datos_muestreo, datos_fisicos, datos_biogeoquimicos], axis=1, join='inner')
                 
                # Reemplaza los NaN por None
                datos_compuesto = datos_compuesto.replace({numpy.nan:None})
                
                
                 
                ## Botón para exportar los resultados
                nombre_archivo =  nombre_programa + '_' + str(anho_consulta) + '.xlsx'
            
                output = BytesIO()
                writer = pandas.ExcelWriter(output, engine='xlsxwriter')
                datos_compuesto.to_excel(writer, index=False, sheet_name='DATOS')
                workbook = writer.book
                worksheet = writer.sheets['DATOS']
                writer.save()
                datos_exporta = output.getvalue()
            
                st.download_button(
                    label="DESCARGA LOS DATOS SELECCIONADOS",
                    data=datos_exporta,
                    file_name=nombre_archivo,
                    help= 'Descarga un archivo .csv con los datos solicitados',
                    mime="application/vnd.ms-excel"
                )
    
    
       
        
        
        
        
        
        
    
    
    
    
      
    
    
###############################################################################
################# PÁGINA DE CONSULTA DE ESTADILLOS ############################
###############################################################################    
    
    
def consulta_estadillos():    
    
    ### Encabezados y titulos 
    #st.set_page_config(page_title='CONSULTA ESTADILLOS', layout="wide",page_icon=logo_IEO_reducido) 
    st.title('Servicio de consulta de estadillos de datos muestreados')
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port
    
    
    # Recupera las tablas de los programas y estaciones disponibles como  dataframes
    conn = init_connection()
    df_programas  = psql.read_sql('SELECT * FROM programas', conn)
    df_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
    conn.close()
    
    
    # Selecciona el programa del que se quieren buscar estadillos
    nombre_programa  = st.selectbox('Selecciona el programa del cual se quiere recuperar el estadillo',(df_programas['nombre_programa']))
    
    id_programa      = int(df_programas['id_programa'][df_programas['nombre_programa']==nombre_programa].values[0])
    
    
    # Determina las fechas de las que hay información de datos de nutrientes
    estaciones_programa = df_estaciones[df_estaciones['programa'] == id_programa]
    
    indices_dataframe   = numpy.arange(0,estaciones_programa.shape[0],1,dtype=int) 
    
    # # Primero recupera los registros correspondientes al periodo evaluado y al año consultado
    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    
    cursor = conn.cursor()
    instruccion_sql = "SELECT id_muestreo,nombre_muestreo,fecha_muestreo,hora_muestreo,estacion,botella,presion_ctd,id_tubo_nutrientes FROM muestreos_discretos INNER JOIN estaciones ON muestreos_discretos.estacion = estaciones.id_estacion WHERE estaciones.programa = %s;"
    cursor.execute(instruccion_sql,(str(id_programa)))
    registros_consulta = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    
    
    dataframe_registros = pandas.DataFrame(registros_consulta, columns=['id_muestreo','nombre_muestreo','fecha_muestreo','hora_muestreo','estacion','botella','presion_ctd','id_tubo_nutrientes'])
    
    # Mantén sólo los registros con datos de id_nutrientes
    dataframe_registros = dataframe_registros[dataframe_registros['id_tubo_nutrientes'].notna()]
    
    # Busca las fechas disponibles 
    dataframe_temporal = dataframe_registros.drop_duplicates('fecha_muestreo')
    listado_fechas     = dataframe_temporal['fecha_muestreo']
    
    if len(listado_fechas) > 0:
    
        # Seleccionas una fecha
        fecha_seleccionada = st.selectbox('Selecciona la fecha de la que se quiere recuperar el estadillo',(listado_fechas))
        
        # Recupera los registros correspondientes a esa fecha
        dataframe_fecha = dataframe_registros[dataframe_registros['fecha_muestreo']==fecha_seleccionada]
        
        # Ajusta el numero de los indices
        indices_dataframe          = numpy.arange(0,dataframe_fecha.shape[0],1,dtype=int)    
        dataframe_fecha['id_temp'] = indices_dataframe
        dataframe_fecha.set_index('id_temp',drop=True,append=False,inplace=True)
        
        # Recupera las coordenadas a partir de la estación asignada
        dataframe_fecha['latitud'] = numpy.zeros(dataframe_fecha.shape[0])
        dataframe_fecha['longitud'] = numpy.zeros(dataframe_fecha.shape[0])
        for idato in range(dataframe_fecha.shape[0]):
            dataframe_fecha['latitud'][idato]  = estaciones_programa['latitud'][estaciones_programa['id_estacion']==dataframe_fecha['estacion'][idato]]
            dataframe_fecha['longitud'][idato] = estaciones_programa['longitud'][estaciones_programa['id_estacion']==dataframe_fecha['estacion'][idato]]
        
        # Recupera las propiedades físicas del registro (temperatura, salinidad....)
        conn = init_connection()
        tabla_registros_fisica    = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
        conn.close()
        dataframe_fecha['temperatura_ctd'] = numpy.zeros(dataframe_fecha.shape[0])
        dataframe_fecha['salinidad_ctd'] = numpy.zeros(dataframe_fecha.shape[0])
        for idato in range(dataframe_fecha.shape[0]):
            dataframe_fecha['temperatura_ctd'][idato]  = tabla_registros_fisica['temperatura_ctd'][tabla_registros_fisica['muestreo']==dataframe_fecha['id_muestreo'][idato]]
            dataframe_fecha['salinidad_ctd'][idato]    = tabla_registros_fisica['salinidad_ctd'][tabla_registros_fisica['muestreo']==dataframe_fecha['id_muestreo'][idato]]
        
        # Quita la columna de estación
        dataframe_fecha = dataframe_fecha.drop(columns=['estacion','id_muestreo'])
        
        # Ajusta el orden de las columnas
        dataframe_fecha = dataframe_fecha[['nombre_muestreo','fecha_muestreo','hora_muestreo','latitud','longitud','botella','id_tubo_nutrientes','presion_ctd','temperatura_ctd','salinidad_ctd']]
        
        # Ordena en función del número de tubo
        dataframe_fecha = dataframe_fecha.sort_values(by=['id_tubo_nutrientes'])
       
        ## Botón para exportar los resultados
        nombre_archivo =  'ESTADILLO_' + nombre_programa + '_' + fecha_seleccionada.strftime("%m/%d/%Y") + '.xlsx'
    
        output = BytesIO()
        writer = pandas.ExcelWriter(output, engine='xlsxwriter')
        dataframe_fecha.to_excel(writer, index=False, sheet_name='DATOS')
        workbook = writer.book
        worksheet = writer.sheets['DATOS']
        writer.save()
        datos_exporta = output.getvalue()
    
        st.download_button(
            label="DESCARGA LOS DATOS SELECCIONADOS",
            data=datos_exporta,
            file_name=nombre_archivo,
            help= 'Descarga un archivo .csv con el estadillo solicitado',
            mime="application/vnd.ms-excel"
        )
        
    else:
        
        texto_error = 'No hay estadillos de entrada correspondientes al programa ' + nombre_programa
        st.warning(texto_error, icon="⚠️")








###############################################################################
################# PÁGINA DE EVOLUCION DEL ANALISIS ############################
###############################################################################    
    
def evolucion_analisis():
    
    st.title('Servicio de consulta de la evolución en los muestreos procesados')
    

    listado_meses = numpy.arange(2,12,dtype=int)

    # Despliega un formulario para elegir el programa y la fecha a consultar
    with st.form("Formulario seleccion"):
        col1, col2 = st.columns(2,gap="small")
        #nombre_programa, tiempo_consulta = st.columns((1, 1))
        with col1:
            tiempo_final_consulta = st.date_input("Selecciona la fecha de finalización del periodo de consulta",datetime.date.today())
        with col2:
            num_meses_previos = st.selectbox("Selecciona el número de meses del periodo de consulta",listado_meses,index=4)
  
        texto_error = 'Para visualizar correctamente los resultados se recomienda evitar periodos de consulta elevados.'
        st.warning(texto_error, icon="⚠️")   
  
        # Botón de envío para confirmar selección
        envio = st.form_submit_button("Enviar")
        
        
    if envio :
    
        tiempo_final_consulta  = datetime.date.today()
        tiempo_inicio_consulta = tiempo_final_consulta + relativedelta(months=-num_meses_previos)
        
        fechas_final_mes   = pandas.date_range(tiempo_inicio_consulta,tiempo_final_consulta,freq='m')
        if tiempo_final_consulta != (fechas_final_mes[-1]).date():
            fechas_mes_actual  = pandas.date_range(fechas_final_mes[-1],tiempo_final_consulta,periods=2)
            fechas_comparacion = fechas_final_mes.union(fechas_mes_actual)
        else:
            fechas_comparacion  = fechas_final_mes  
        
        # Recupera la tabla de los programas disponibles como un dataframe
        conn         = init_connection()
        df_programas = psql.read_sql('SELECT * FROM programas', conn)
        
        # Recupera la tabla del estado de los procesos como un dataframe
        temporal_estado_procesos = psql.read_sql('SELECT * FROM estado_procesos', conn)
        conn.close()
        
        # Estados
        nombre_estados  = ['No disponible','Pendiente de análisis','Analizado','Post-Procesado']
        colores_estados = ['#CD5C5C','#F4A460','#87CEEB','#66CDAA','#2E8B57']
        
        
        
        # Contador
        num_valores = numpy.zeros((len(fechas_comparacion),df_programas.shape[0],len(nombre_estados)),dtype=int)
        
        for itiempo in range(len(fechas_comparacion)):
            
            tiempo_consulta = fechas_comparacion[itiempo]
            
            for iprograma in range(df_programas.shape[0]):
            
                # Extrae los datos disponibles del programa consultado 
                estado_procesos_programa = temporal_estado_procesos[temporal_estado_procesos['programa']==iprograma+1]
                
                # Bucle if para desplegar información únicamente si hay información del programa seleccionado
                if estado_procesos_programa.shape[0] != 0:
                            
                    # Quita del dataframe las columnas con el identificador del programa y el número registro (no interesa mostrarlo en la web)
                    estado_procesos_programa = estado_procesos_programa.drop(['id_proceso','programa'], axis = 1)
                    
                    # Reemplaza los nan por None
                    estado_procesos_programa = estado_procesos_programa.fillna(numpy.nan).replace([numpy.nan], [None])
                    
                    # Actualiza el indice del dataframe 
                    indices_dataframe         = numpy.arange(0,estado_procesos_programa.shape[0],1,dtype=int)
                    estado_procesos_programa['id_temp'] = indices_dataframe
                    estado_procesos_programa.set_index('id_temp',drop=True,append=False,inplace=True)
                    
                    
                    ### Determina el estado de cada proceso, en la fecha seleccionada
                    estado_procesos_programa['id_estado']    = [None]*estado_procesos_programa.shape[0]
                    
            
                    for ianho in range(estado_procesos_programa.shape[0]):
                    
                        # Caso 3. Fecha de consulta posterior al post-procesado.
                        if pandas.isnull(estado_procesos_programa['fecha_post_procesado'][ianho]) is False:
                            if tiempo_consulta.date() >= (estado_procesos_programa['fecha_post_procesado'][ianho]):     
                                estado_procesos_programa['id_estado'][ianho] = 3
                        else:
                            
                            # Caso 2. Fecha de consulta posterior al análisis de laboratorio pero anterior a realizar el post-procesado.
                            if pandas.isnull(estado_procesos_programa['fecha_analisis_laboratorio'][ianho]) is False:
                                if tiempo_consulta.date() >= (estado_procesos_programa['fecha_analisis_laboratorio'][ianho]):  # estado_procesos_programa['fecha_analisis_laboratorio'][ianho] is not None:     
                                    estado_procesos_programa['id_estado'][ianho] = 2
                                else:
                                    if tiempo_consulta.date()  >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
                                        estado_procesos_programa['id_estado'][ianho] = 1 
                                                                            
                            else:
                                # Caso 1. Fecha de consulta posterior a terminar la campaña pero anterior al análisis en laboratorio, o análisis no disponible. 
                                if pandas.isnull(estado_procesos_programa['fecha_entrada_datos'][ianho]) is False:
                                    if tiempo_consulta.date()  >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
                                        estado_procesos_programa['id_estado'][ianho] = 1 
                                        
                                
                    # Cuenta el numero de veces que se repite cada estado para sacar un gráfico pie-chart
            
                    for ivalor in range(len(nombre_estados)):
                        try:
                            num_valores[itiempo,iprograma,ivalor] = estado_procesos_programa['id_estado'].value_counts()[ivalor]
                        except:
                            pass
        
                
        
        
        fig, ax           = plt.subplots()
        anchura_barra     = 0.125
        etiquetas         = df_programas['abreviatura']
        id_mes            = numpy.arange(0,len(fechas_comparacion))
        
        nombres_meses     =['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
        vector_nombres    = [None]*fechas_comparacion.shape[0]
        for idato in range(fechas_comparacion.shape[0]):
            vector_nombres[idato] = nombres_meses[fechas_comparacion[idato].month-1] + ' ' + fechas_comparacion[idato].strftime("%y")
        
        # Bucle con cada programa de muestreo
        valor_maximo_programa = numpy.zeros(df_programas.shape[0])
        for iprograma in range(df_programas.shape[0]): 
            
            # Desplaza la coordenada x de cada programa para representarlo separado de los demás
            posicion_x_programa    = id_mes + anchura_barra*(iprograma - 2)
            
            # Extrae los valores de muestras en cada estado para el programa correspondiente
            valores_programa   = num_valores[:,iprograma,:]
            
            # Busca la posición del fondo de cada barra a partir de los valores de la barra anterior
            valores_acumulados = numpy.cumsum(valores_programa,axis =1)
            acumulados_mod     = numpy.c_[ numpy.zeros(valores_acumulados.shape[0]), valores_acumulados]
            acumulados_mod     = numpy.delete(acumulados_mod, -1, axis = 1)
        
            # Determina la posición de las etiquetas y la máxima altura (para luego definir el rango del eje y)
            etiqueta_altura                   = valores_acumulados[:,-1] + 1.5
            valor_maximo_programa [iprograma] = max(etiqueta_altura)
            
            # Representa la barra correspondiente a cada estado, en los distintos tiempos considerados
            for igrafico in range(num_valores.shape[2]):
                
                posicion_fondo = acumulados_mod[:,igrafico]
                
                plt.bar(posicion_x_programa, num_valores[:,iprograma,igrafico], anchura_barra, bottom = posicion_fondo ,color=colores_estados[igrafico],edgecolor='k')
        
            # Añade una etiqueta para identificar al programa
            etiqueta_nombre   = [etiquetas[iprograma]]*num_valores.shape[0]
            for ibarra in range(num_valores.shape[0]):
                # Etiqueta con el nombre del programa
                ax.text(posicion_x_programa[ibarra], etiqueta_altura[ibarra], etiqueta_nombre[ibarra], ha="center", va="bottom")
                # Etiqueta con el valor de cada uno de los estados
                if valores_programa[ibarra,0] > 0:
                    ax.text(posicion_x_programa[ibarra], valores_acumulados[ibarra,0] , str(valores_programa[ibarra,0]), ha="center", va="bottom")
                if valores_programa[ibarra,1] > 0:
                    ax.text(posicion_x_programa[ibarra], valores_acumulados[ibarra,1] , str(valores_programa[ibarra,1]), ha="center", va="bottom")
                if valores_programa[ibarra,2] > 0:
                    ax.text(posicion_x_programa[ibarra], valores_acumulados[ibarra,2] , str(valores_programa[ibarra,2]), ha="center", va="bottom")
                if valores_programa[ibarra,3] > 0:
                    ax.text(posicion_x_programa[ibarra], valores_acumulados[ibarra,3] , str(valores_programa[ibarra,3]), ha="center", va="bottom")
        
        # Cambia el nombre de los valores del eje X. De nºs enteros al mes correspondiente
        plt.xticks(id_mes,vector_nombres)
        
        # Ajusta límites del gráfico
        plt.ylim([0, max(valor_maximo_programa)+2])
             
        # Ajusta tamaño 
        fig.set_size_inches(16.5, 5.5)
        
        # Añade leyenda
        plt.legend(nombre_estados, bbox_to_anchor = (0.85, 1.085), ncol=len(nombre_estados)) # ,loc="upper left"
        
        # Añade nombre a los ejes
        plt.xlabel('Fecha')
        plt.ylabel('Años de muestreo')
        
        # Añade un cuadro de texto explicado las abreviaturas
        textstr = ''
        for iprograma in range(df_programas.shape[0]):
            textstr = textstr + df_programas['abreviatura'][iprograma] + '=' + df_programas['nombre_programa'][iprograma] + '; '
        #textstr = 'P=PELACUS RV = RADIALES VIGO RC = RADIALES CORUÑA  RS = RADIALES SANTANDER RP = RADPROF '
        ax.text(0.15, 1.125, textstr, transform=ax.transAxes, fontsize=11,
                verticalalignment='top', bbox={'edgecolor':'none','facecolor':'white'})
               
        buf = BytesIO()
        fig.savefig(buf, format="png",bbox_inches='tight')
        st.image(buf)       
        
        st.download_button("DESCARGAR GRÁFICO",buf,'GRAFICO.png')
        
        
        
        
        
        
        
###############################################################################
############## PÁGINA PARA INTRODUCIR LAS MUESTRAS EN PROCESO #################
###############################################################################    
    
def actualiza_procesos():

    # Despliega un botón lateral para seleccionar el tipo de información a introducir       
    entradas     = ['Nuevas muestras a procesar', 'Procesado terminado']
    tipo_entrada = st.sidebar.radio("Indicar la información a introducir",entradas)

    fecha_actual = datetime.date.today()

    if tipo_entrada == entradas[0]:

        st.header('Añadir muestras en proceso')
        
        # Busca el año actual para limitar la fecha de entrada 
        anho_actual = fecha_actual.year
    
        # Recupera la tabla de los programas disponibles como un dataframe
        conn = init_connection()
        df_programas = psql.read_sql('SELECT * FROM programas', conn)
        conn.close()
       
        # Despliega un formulario para introducir los datos de las muestras que se están analizando
        with st.form("Formulario seleccion"):
        
            descripcion_muestras = st.text_input('Descipción de las muestras', value="")
            
            col1, col2, col3= st.columns(3,gap="small")
            with col1:
                num_muestras = st.number_input('Número de muestras:',format='%i',value=round(1),min_value=1)
                num_muestras = round(num_muestras)
            with col2:
                nombre_programa  = st.selectbox('Selecciona el programa',(df_programas['nombre_programa']))
                # Recupera el identificador del programa seleccionado
                id_programa_elegido = int(df_programas['id_programa'][df_programas['nombre_programa']==nombre_programa].values[0])
    
            with col3:
                anho_consulta = st.number_input('Año:',format='%i',value=anho_actual,max_value=anho_actual)
        
            fecha_estimada_fin = st.date_input('Fecha estimada de finalizacion',min_value=fecha_actual,value=fecha_actual)
        
            # Botón de envío para confirmar selección
            submit = st.form_submit_button("Enviar")
    
            if submit == True:
                
                conn = init_connection()
                cursor = conn.cursor()           
                instruccion_sql = "INSERT INTO procesado_actual_nutrientes (nombre_proceso,programa,nombre_programa,año,num_muestras,fecha_inicio,fecha_estimada_fin,fecha_real_fin,io_estado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (nombre_proceso) DO UPDATE SET (programa,año,nombre_programa,num_muestras,fecha_inicio,fecha_estimada_fin,fecha_real_fin,io_estado) = (EXCLUDED.programa,EXCLUDED.año,EXCLUDED.nombre_programa,EXCLUDED.num_muestras,EXCLUDED.fecha_inicio,EXCLUDED.fecha_estimada_fin,EXCLUDED.fecha_real_fin,EXCLUDED.io_estado);"   
                cursor.execute(instruccion_sql, (descripcion_muestras,id_programa_elegido,nombre_programa,anho_consulta,num_muestras,fecha_actual,fecha_estimada_fin,None,1)) 
                conn.commit() 
                cursor.close()
                conn.close()  
                
                texto_exito = 'Muestras ' + descripcion_muestras + ' añadidas a la cola de procesado'
                st.success(texto_exito)


    if tipo_entrada == entradas[1]:
        
        st.subheader('Listado de análisis en curso')
        
        # Muestra el listado de los análisis en curso 
        altura_tabla       = 200 # Altura de la tabla con los procesos en curso 
        
        df_muestreos_curso = estado_procesos(altura_tabla)

        if df_muestreos_curso.shape[0] > 0:

            # Despliega una selección del análisis a marcar como finalizado
            with st.form("Formulario seleccion"):
                       
                nombre_muestra_terminada  = st.selectbox('Selecciona el análisis terminado',(df_muestreos_curso['Muestras']))
    
                submit = st.form_submit_button("Enviar")
    
                if submit == True:
                    
                    fecha_actual = datetime.date.today()
                    
                    conn = init_connection()
                    cursor = conn.cursor() 
                    instruccion_sql = "UPDATE procesado_actual_nutrientes SET io_estado = %s,fecha_real_fin = %s WHERE nombre_proceso = %s;"
                    cursor.execute(instruccion_sql, (int(0),fecha_actual,nombre_muestra_terminada))                
                    conn.commit() 
                    cursor.close()
                    conn.close()  
                    
                    texto_exito = 'Estado de las muestras ' + nombre_muestra_terminada + ' actualizado correctamente'
                    st.success(texto_exito)
                    
                    st.experimental_rerun()

    
###############################################################################
#################### PÁGINA DE PROCESOS EN CURSO ##############################
###############################################################################    
    
def consulta_procesos():
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    entradas     = ['Procesos actualmente en curso', 'Procesado realizados en un periodo de tiempo']
    tipo_entrada = st.sidebar.radio("Indicar la consulta a realizar",entradas)



    # Consulta procesos actualmente en curso
    if tipo_entrada == entradas[0]:    
    
        st.subheader('Listado de análisis en curso')
        
        # Muestra el listado de los análisis en curso 
        altura_tabla       = 300 # Altura de la tabla con los procesos en curso 
        estado_procesos(altura_tabla)
        
        
        
    # Consulta procesos realizados entre dos fechas
    if tipo_entrada == entradas[1]:  
        
        st.subheader('Listado de procesos realizados y en curso durante un periodo de tiempo')
        
        fecha_actual         = datetime.date.today()
        fecha_inicio_defecto = fecha_actual - datetime.timedelta(days=30)
        
        altura_tabla       = 300 # Altura de las tablas con información de los procesos
        
        # Despliega un formulario para seleccionar las fechas de inicio y final
        with st.form("Formulario seleccion"):
                   
            col1, col2= st.columns(2,gap="small")
            
            with col1:
                fecha_inicio_consulta = st.date_input('Fecha de incio del periodo de consulta',max_value=fecha_actual,value=fecha_inicio_defecto)
            with col2:
                fecha_final_consulta = st.date_input('Fecha de finalización del periodo de consulta',max_value=fecha_actual,value=fecha_actual)

            submit = st.form_submit_button("Consultar")
    
            if submit == True:

                # Recupera todos los muestreos almacenados 
                conn = init_connection()
                df_muestreos = psql.read_sql('SELECT * FROM procesado_actual_nutrientes', conn)
                conn.close()
                
                # Renombra las columnas
                df_muestreos = df_muestreos.rename(columns={'nombre_proceso':'Muestras','nombre_programa':'Programa','año':'Año','num_muestras':'Número muestras','fecha_inicio':'Inicio','fecha_estimada_fin':'Final estimado','fecha_real_fin':'Final real'})
                
                # Genera un dataframe con los procesos terminados entre ambas fechas, elimina las columnas que no interesa mostrar, define una columna índice y ajusta el formato de las fechas
                df_muestreos_terminados = df_muestreos.loc[(df_muestreos['Final real'] >= fecha_inicio_consulta) & (df_muestreos['Final real'] <= fecha_final_consulta)]
                df_muestreos_terminados = df_muestreos_terminados.drop(columns=['id_proceso','programa','io_estado'])
                
                indices_dataframe                 = numpy.arange(0,df_muestreos_terminados.shape[0],1,dtype=int)
                df_muestreos_terminados['indice'] = indices_dataframe
                df_muestreos_terminados.set_index('indice',drop=True,append=False,inplace=True)
                
                for idato in range(df_muestreos_terminados.shape[0]):
                    df_muestreos_terminados['Inicio'][idato]         =  df_muestreos_terminados['Inicio'][idato].strftime("%Y-%m-%d")
                    df_muestreos_terminados['Final estimado'][idato] =  df_muestreos_terminados['Final estimado'][idato].strftime("%Y-%m-%d")    
                    df_muestreos_terminados['Final real'][idato]     =  df_muestreos_terminados['Final real'][idato].strftime("%Y-%m-%d")    
                                
            
                # Genera un dataframe con los procesos en curso, elimina las columnas que no interesa mostrar, define una columna índice y ajusta el formato de las fechas
                if fecha_final_consulta == fecha_actual:
                    df_muestreos_curso = df_muestreos[df_muestreos['io_estado']==1]
                
                else: 
                    df_muestreos_curso = df_muestreos.loc[(df_muestreos['Final real'] >= fecha_final_consulta) & (df_muestreos['Inicio'] >= fecha_inicio_consulta)]

                df_muestreos_curso = df_muestreos_curso.drop(columns=['id_proceso','programa','io_estado'])
                
                indices_dataframe            = numpy.arange(0,df_muestreos_curso.shape[0],1,dtype=int)
                df_muestreos_curso['indice'] = indices_dataframe
                df_muestreos_curso.set_index('indice',drop=True,append=False,inplace=True)
                
                for idato in range(df_muestreos_curso.shape[0]):
                    df_muestreos_curso['Inicio'][idato]         =  df_muestreos_curso['Inicio'][idato].strftime("%Y-%m-%d")
                    df_muestreos_curso['Final estimado'][idato] =  df_muestreos_curso['Final estimado'][idato].strftime("%Y-%m-%d")
                                  
                # Muestra sendos dataframes
                
                st.subheader('Listado de procesos en curso')
                if df_muestreos_curso.shape[0] > 0:
                        
                    # Muestra una tabla con los análisis en curso
                    altura_tabla = 150
                    gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(df_muestreos_curso)
                    gridOptions = gb.build()
                    st_aggrid.AgGrid(df_muestreos_curso,gridOptions=gridOptions,height = altura_tabla,enable_enterprise_modules=True,allow_unsafe_jscode=True)    
            
                else:
                    
                    texto_error = 'No hay ninguna muestra en proceso durante el periodo de tiempo consultado (' + fecha_inicio_consulta.strftime("%Y/%m/%d") + '-' + fecha_final_consulta.strftime("%Y/%m/%d") + ')'
                    st.warning(texto_error, icon="⚠️") 

                st.subheader('Listado de procesos terminados')

                if df_muestreos_terminados.shape[0] > 0:
                        
                    # Muestra una tabla con los análisis en curso
                    altura_tabla = 300
                    gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(df_muestreos_terminados)
                    gridOptions = gb.build()
                    st_aggrid.AgGrid(df_muestreos_terminados,gridOptions=gridOptions,height = altura_tabla,enable_enterprise_modules=True,allow_unsafe_jscode=True)    
            
                else:
                    
                    texto_error = 'No se terminó ningún análisis durante el periodo de tiempo consultado (' + fecha_inicio_consulta.strftime("%Y/%m/%d") + '-' + fecha_final_consulta.strftime("%Y/%m/%d") + ')'
                    st.warning(texto_error, icon="⚠️")  
                
                    

        
 





###############################################################################
################# PÁGINA DE ENTRADA DE SALIDAS A MAR ##########################
###############################################################################    
    
def entrada_salidas_mar():
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    entradas     = ['Añadir salida al mar', 'Añadir personal participante','Consultar o modificar salidas realizadas']
    tipo_entrada = st.sidebar.radio("Indicar la consulta a realizar",entradas)
    

    # Añade una salida al mar
    if tipo_entrada == entradas[0]:  
        
        st.subheader('Salida al mar')
        
        # Recupera la tabla con los buques disponibles en la base de datos, como un dataframe
        conn = init_connection()
        df_buques = psql.read_sql('SELECT * FROM buques', conn)
        conn.close()
        
        # Recupera tablas con información utilizada
        conn                       = init_connection()
        
        # Personal disponible
        df_personal                = psql.read_sql('SELECT * FROM personal_salidas', conn)
        df_personal_comisionado    = df_personal[df_personal['comisionado']==True]
        df_personal_no_comisionado = df_personal[df_personal['comisionado']==False]
        # Salidas realizadas 
        df_salidas = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_salidas_radiales = df_salidas[df_salidas['nombre_programa']=='RADIAL CORUÑA']
        # Estaciones de muestreo (radiales)
        df_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
        df_estaciones_radiales = df_estaciones[df_estaciones['programa']==3]
        
        conn.close()
        

        # tipos de salida en las radiales
        tipos_radiales = ['Mensual','Semanal']
        
        
        fecha_actual        = datetime.date.today()
        
        hora_defecto_inicio = datetime.time(8,30,0,0,tzinfo = datetime.timezone.utc)
        hora_defecto_final  = datetime.time(14,30,0,0,tzinfo = datetime.timezone.utc)
        
        # Despliega un formulario para seleccionar las fechas de inicio y final
        with st.form("Formulario seleccion"):
                   
            nombre_salida        = st.text_input('Nombre de la salida', value="")
            
            col1, col2,col3= st.columns(3,gap="small")
            
            with col1:
                
                tipo_salida     = st.selectbox('Tipo de radial',(tipos_radiales))
                
                buque_elegido = st.selectbox('Selecciona el buque utilizado',(df_buques['nombre_buque']))
                id_buque_elegido = int(df_buques['id_buque'][df_buques['nombre_buque']==buque_elegido].values[0])               

                   
            with col2:               
                fecha_salida  = st.date_input('Fecha de salida',max_value=fecha_actual,value=fecha_actual)

                hora_salida   = st.time_input('Hora de salida (UTC)', value=hora_defecto_inicio)

            with col3:
                
                fecha_regreso = st.date_input('Fecha de regreso',max_value=fecha_actual,value=fecha_actual)

                hora_regreso  = st.time_input('Hora de regreso (UTC)', value=hora_defecto_final)


            personal_comisionado    = st.multiselect('Personal comisionado participante',df_personal_comisionado['nombre_apellidos'])
            json_comisionados       = json.dumps(personal_comisionado)

            personal_no_comisionado = st.multiselect('Personal no comisionado participante',df_personal_no_comisionado['nombre_apellidos'])
            json_no_comisionados    = json.dumps(personal_no_comisionado)
            
            estaciones_muestreadas  = st.multiselect('Estaciones muestreadas',df_estaciones_radiales['nombre_estacion'])
            json_estaciones         = json.dumps(estaciones_muestreadas)



            observaciones = st.text_input('Observaciones', value="")

            submit = st.form_submit_button("Añadir salida")

    
            if submit == True:
                
                # Comprueba los datos introducidos
                io_error = 0
                if not nombre_salida:
                    io_error = 1
                    texto_error = 'La salida debe contener un nombre'
                    
                if fecha_regreso == fecha_salida and hora_regreso == hora_salida:
                    io_error = 1
                    texto_error = 'La fecha y hora de regreso no puede ser la misma que la de partida'

                if fecha_regreso < fecha_salida :
                    io_error = 1
                    texto_error = 'La fecha de regreso no puede ser anterior a la de partida'
                                
                if io_error == 1:
                    st.warning(texto_error, icon="⚠️")
                
                else:
                
                    io_incluido = 0
                    for isalida in range(df_salidas_radiales.shape[0]):
                        if df_salidas_radiales['fecha_salida'][isalida] == fecha_salida and df_salidas_radiales['tipo_salida'][isalida] == tipo_salida:
                            io_incluido = 1
    
                    if io_incluido == 0:                     
                        
                        instruccion_sql = '''INSERT INTO salidas_muestreos (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,hora_salida,fecha_retorno,hora_retorno,buque,participantes_comisionados,participantes_no_comisionados,observaciones,estaciones)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_salida) DO UPDATE SET (nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,hora_salida,fecha_retorno,hora_retorno,buque,participantes_comisionados,participantes_no_comisionados,observaciones,estaciones) = ROW(EXCLUDED.nombre_salida,EXCLUDED.programa,EXCLUDED.nombre_programa,EXCLUDED.tipo_salida,EXCLUDED.fecha_salida,EXCLUDED.hora_salida,EXCLUDED.fecha_retorno,EXCLUDED.hora_retorno,EXCLUDED.buque,EXCLUDED.participantes_comisionados,EXCLUDED.participantes_no_comisionados,EXCLUDED.observaciones,EXCLUDED.estaciones);''' 
                                
                        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                        cursor = conn.cursor()
                        cursor.execute(instruccion_sql, (nombre_salida,3,'RADIAL CORUÑA',tipo_salida,fecha_salida,hora_salida,fecha_regreso,hora_regreso,id_buque_elegido,json_comisionados,json_no_comisionados,observaciones,json_estaciones))
                        conn.commit()
                        cursor.close()
                        conn.close()
        
                        texto_exito = 'Salida añadida correctamente'
                        st.success(texto_exito)
                        
                    else:
                        texto_error = 'La base de datos ya contiene una salida ' + tipo_salida.lower() + ' para la fecha seleccionada'
                        st.warning(texto_error, icon="⚠️")                      
                        
    




    # Añade personal participante en las salidas de radial
    if tipo_entrada == entradas[1]: 

        st.subheader('Personal participante')
        
        # Recupera la tabla con el personal ya introducido, como un dataframe
        conn = init_connection()
        df_personal = psql.read_sql('SELECT * FROM personal_salidas', conn)
        conn.close()

        # Despliega un formulario para introducir los datos
        with st.form("Formulario seleccion"):
                   
            nombre_participante  = st.text_input('Nombre y apellidos del nuevo personal', value="")

            correo_participante  = st.text_input('Correo del nuevo personal', value="")
            
            comision             = st.checkbox('Comisionado')
            
            submit = st.form_submit_button("Añadir participante")

            if submit == True:

                io_incluido = 0
                for ipersonal in range(df_personal.shape[0]):
                    if df_personal['nombre_apellidos'][ipersonal] == nombre_participante:
                        io_incluido = 1
                
                if io_incluido == 0:

                    instruccion_sql = '''INSERT INTO personal_salidas (nombre_apellidos,correo,comisionado)
                        VALUES (%s,%s,%s) ON CONFLICT (id_personal) DO UPDATE SET (nombre_apellidos,correo,comisionado) = ROW(EXCLUDED.nombre_apellidos,EXCLUDED.correo,EXCLUDED.comisionado);''' 
                            
                    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()
                    cursor.execute(instruccion_sql, (nombre_participante,correo_participante,comision))
                    conn.commit()
                    cursor.close()
                    conn.close()

                    texto_exito = 'Participante añadido correctamente'
                    st.success(texto_exito)
        
                else:
                    texto_error = 'El participante introducido ya se encuentra en la base de datos '
                    st.warning(texto_error, icon="⚠️")  


    # Consulta las salidas realizadas
    if tipo_entrada == entradas[2]: 
        
        st.subheader('Salidas al mar realizadas')

        # Muestra las salidas realizadas

        # Recupera la tabla con las salidas disponibles, como un dataframe
        conn = init_connection()
        df_salidas = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_salidas_radiales = df_salidas[df_salidas['nombre_programa']=='RADIAL CORUÑA']
        df_buques = psql.read_sql('SELECT * FROM buques', conn)
        conn.close()
        
        # Añade una columna con el nombre del buque utilizado
        df_salidas_radiales['Buque'] = [None]*df_salidas_radiales.shape[0]
        for isalida in range(df_salidas_radiales.shape[0]):
            df_salidas_radiales['Buque'][isalida] = df_buques['nombre_buque'][df_buques['id_buque']==df_salidas_radiales['buque'].iloc[isalida]]
        
        # Elimina las columnas que no interesa mostrar
        df_salidas_radiales = df_salidas_radiales.drop(columns=['id_salida','programa','nombre_programa','buque'])
    
        # Renombra las columnas
        df_salidas_radiales = df_salidas_radiales.rename(columns={'nombre_salida':'Salida','tipo_salida':'Tipo','fecha_salida':'Fecha salida','hora_salida':'Hora salida','fecha_retorno':'Fecha retorno','hora_retorno':'Hora retorno','observaciones':'Observaciones','estaciones':'Estaciones muestreadas','participantes_comisionados':'Participantes comisionados','participantes_no_comisionados':'Participantes no comisionados'})
    
        # Ajusta el formato de las fechas
        for idato in range(df_salidas_radiales.shape[0]):
            df_salidas_radiales['Fecha salida'].iloc[idato]   =  df_salidas_radiales['Fecha salida'].iloc[idato].strftime("%Y-%m-%d")
            df_salidas_radiales['Fecha retorno'].iloc[idato]  =  df_salidas_radiales['Fecha retorno'].iloc[idato].strftime("%Y-%m-%d")

        # Ordena los valores por fechas
        df_salidas_radiales = df_salidas_radiales.sort_values('Fecha salida')

        # Mueve os identificadores de muestreo al final del dataframe
        listado_cols = df_salidas_radiales.columns.tolist()
        listado_cols.append(listado_cols.pop(listado_cols.index('Observaciones')))
        #listado_cols.insert(0, listado_cols.pop(listado_cols.index('longitud')))    
        df_salidas_radiales = df_salidas_radiales[listado_cols]
          
        
        # Muestra una tabla con las salidas realizadas
        gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(df_salidas_radiales)
        gridOptions = gb.build()
        st_aggrid.AgGrid(df_salidas_radiales,gridOptions=gridOptions,enable_enterprise_modules=True,allow_unsafe_jscode=True,reload_data=True)    


        # Modifica una salida
        st.subheader('Modifica salida al mar')
        
        # Recupera la tabla con los buques disponibles en la base de datos, como un dataframe
        conn = init_connection()
        df_buques = psql.read_sql('SELECT * FROM buques', conn)
        conn.close()
          
        # Recupera tablas con información utilizada
        conn                       = init_connection()
          
        # Personal disponible
        df_personal                = psql.read_sql('SELECT * FROM personal_salidas', conn)
        df_personal_comisionado    = df_personal[df_personal['comisionado']==True]
        df_personal_no_comisionado = df_personal[df_personal['comisionado']==False]
        # Salidas realizadas 
        df_salidas = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_salidas_radiales = df_salidas[df_salidas['nombre_programa']=='RADIAL CORUÑA']
        # Estaciones de muestreo (radiales)
        df_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
        df_estaciones_radiales = df_estaciones[df_estaciones['programa']==3]
        
        conn.close()
      
        # Recupera tablas con informacion utilizada en el procesado
        conn                = init_connection()
        df_salidas          = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas        = psql.read_sql('SELECT * FROM programas', conn)
        conn.close()    
        
        id_radiales   = df_programas.index[df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
        #id_radiales   = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].iloc[0]


        fecha_actual        = datetime.date.today()
        
        hora_defecto_inicio = datetime.time(8,30,0,0,tzinfo = datetime.timezone.utc)
        hora_defecto_final  = datetime.time(14,30,0,0,tzinfo = datetime.timezone.utc)
        
        
        # Despliega menús de selección del programa, tipo de salida, año y fecha               
        col1, col2, col3= st.columns(3,gap="small")
     
        with col1: 
            programa_seleccionado     = st.selectbox('Programa',(df_programas['nombre_programa']),index=id_radiales)   
            df_salidas_seleccion      = df_salidas[df_salidas['nombre_programa']==programa_seleccionado]
            
        
        with col2:
            tipo_salida_seleccionada  = st.selectbox('Tipo de salida',(df_salidas_seleccion['tipo_salida'].unique()))   
            df_salidas_seleccion      = df_salidas_seleccion[df_salidas_seleccion['tipo_salida']==tipo_salida_seleccionada]
        
            # Añade la variable año al dataframe
            indices_dataframe               = numpy.arange(0,df_salidas_seleccion.shape[0],1,dtype=int)    
            df_salidas_seleccion['id_temp'] = indices_dataframe
            df_salidas_seleccion.set_index('id_temp',drop=False,append=False,inplace=True)
            
            # Define los años con salidas asociadas
            df_salidas_seleccion['año'] = numpy.zeros(df_salidas_seleccion.shape[0],dtype=int)
            for idato in range(df_salidas_seleccion.shape[0]):
                df_salidas_seleccion['año'][idato] = df_salidas_seleccion['fecha_salida'][idato].year 
            df_salidas_seleccion       = df_salidas_seleccion.sort_values('fecha_salida')
            
            listado_anhos              = df_salidas_seleccion['año'].unique()
        
        with col3:
            anho_seleccionado           = st.selectbox('Año',(listado_anhos),index=len(listado_anhos)-1)
            df_salidas_seleccion        = df_salidas_seleccion[df_salidas_seleccion['año']==anho_seleccionado]
    
        salida                      = st.selectbox('Muestreo',(df_salidas_seleccion['nombre_salida']),index=df_salidas_seleccion.shape[0]-1)   
    
        # Recupera el identificador de la salida seleccionada
        id_salida                   = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==salida].iloc[0]
    
        fecha_salida                = df_salidas_seleccion['fecha_salida'][df_salidas_seleccion['nombre_salida']==salida].iloc[0]
    

        # Despliega un formulario para modificar los datos de la salida 
        with st.form("Formulario seleccion"):
                   
            
            col1, col2,col3= st.columns(3,gap="small")
            
            with col1:
                                
                buque_elegido = st.selectbox('Selecciona el buque utilizado',(df_buques['nombre_buque']))
                id_buque_elegido = int(df_buques['id_buque'][df_buques['nombre_buque']==buque_elegido].values[0])               

                   
            with col2:               
                fecha_salida  = st.date_input('Fecha de salida',max_value=fecha_actual,value=fecha_salida)

                hora_salida   = st.time_input('Hora de salida (UTC)', value=hora_defecto_inicio)

            with col3:
                
                fecha_regreso = st.date_input('Fecha de regreso',max_value=fecha_actual,value=fecha_salida)

                hora_regreso  = st.time_input('Hora de regreso (UTC)', value=hora_defecto_final)


            personal_comisionado    = st.multiselect('Personal comisionado participante',df_personal_comisionado['nombre_apellidos'])
            json_comisionados       = json.dumps(personal_comisionado)

            personal_no_comisionado = st.multiselect('Personal no comisionado participante',df_personal_no_comisionado['nombre_apellidos'])
            json_no_comisionados    = json.dumps(personal_no_comisionado)
            
            estaciones_muestreadas  = st.multiselect('Estaciones muestreadas',df_estaciones_radiales['nombre_estacion'])
            json_estaciones         = json.dumps(estaciones_muestreadas)

            observaciones = st.text_input('Observaciones', value="")
    
            submit = st.form_submit_button("Actualizar salida")
    
            if submit == True:
                           
                instruccion_sql = '''UPDATE salidas_muestreos SET fecha_salida = %s, hora_salida = %s, fecha_retorno = %s,hora_retorno = %s, buque = %s, participantes_comisionados = %s ,participantes_no_comisionados = %s,observaciones = %s,estaciones = %s WHERE id_salida = %s;''' 
                        
                conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()
                cursor.execute(instruccion_sql, (fecha_salida,hora_salida,fecha_regreso,hora_regreso,int(id_buque_elegido),json_comisionados,json_no_comisionados,observaciones,json_estaciones,int(id_salida)))
                conn.commit()
                cursor.close()
                conn.close()


                texto_exito = 'Salida ' + salida + ' actualizada correctamente'
                st.success(texto_exito)
                
                time.sleep(5)
                    
                st.experimental_rerun()





###############################################################################
################# PÁGINA DE ENTRADA DE DATOS DEL ESTADO DEL  MAR ##############
###############################################################################    

def entrada_condiciones_ambientales():
    
    st.subheader('Condiciones ambientales')
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port
    
    # Recupera la tabla de las salidas realizadas 
    conn          = init_connection()
    df_salidas    = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    df_programas  = psql.read_sql('SELECT * FROM programas', conn)
    df_estaciones = psql.read_sql('SELECT * FROM estaciones', conn)
    conn.close()
    
    id_radiales            = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].iloc[0]

    df_salidas_radiales    = df_salidas[df_salidas['programa']==id_radiales] 
    df_estaciones_radiales = df_estaciones[df_estaciones['programa']==id_radiales]
    
    # Ajusta el formato de las fechas
    for idato in range(df_salidas_radiales.shape[0]):
        df_salidas_radiales['fecha_salida'].iloc[idato]  =  df_salidas_radiales['fecha_salida'].iloc[idato].strftime("%Y-%m-%d")
   
    # Selecciona la salida de la que se quiere introducir datos
    df_salidas_radiales    = df_salidas_radiales.sort_values('fecha_salida',ascending=False)
    salida                 = st.selectbox('Salida',(df_salidas_radiales['nombre_salida']))   
    id_salida              = df_salidas_radiales['id_salida'][df_salidas_radiales['nombre_salida']==salida].iloc[0]

    # Extrae las estaciones visitadas en la salida seleccionada
    listado_estaciones = df_salidas_radiales['estaciones'][df_salidas_radiales['id_salida']==id_salida].iloc[0] 

    if listado_estaciones is not None:
                
        # Vectores con los valores de las variables que tienen opciones concretas
        seleccion_SN = ['Si','No']
        direcciones  = ['N','NW','W','SW','S','SE','E','NE'] 

        # doglas_nombre  = ['Calma (0)','Rizada (1)','Marejadilla (2)','Marejada (3)','Fuerte marejada (4)','Gruesa (5)','Muy Gruesa (6)','Arbolada (7)','Montañosa (8)','Enorme (9)']
        # douglas_hmin   = [0,0,0.1,0.5,1.25,2.5,4,6,9,14]
        # douglas_hmax   = [0,0.1,0.5,1.25,2.5,4,6,9,14,100]

        beaufort_nombre = ['Calma (0)','Ventolina (1)','Brisa muy débil (2)','Brisa Ligera (3)','Brisa moderada (4)','Brisa fresca (5)','Brisa fuerte (6)','Viento fuerte (7)','Viento duro (8)','Muy duro (9)','Temporal (10)','Borrasca (11)','Huracán (12)']
        beaufort_vmin   = [0,2,6,12,20,29,39,50,62,75,89,103,118]
        beaufort_vmax   = [2,6,12,20,29,39,50,62,75,89,103,118,500]

        mareas          = ['Bajamar','Media','Pleamar']

        # Selecciona la estación de la que se quiere introducir datos (entre todas las disponibles)
        estacion_elegida    = st.selectbox('Estacion',(listado_estaciones))
        id_estacion_elegida = int(df_estaciones_radiales['id_estacion'][df_estaciones_radiales['nombre_estacion']==estacion_elegida].values[0])
    
        # recupera los datos disponibles en la base de datos para asignar valores por defecto
        conn                     = init_connection()
        df_condiciones           = psql.read_sql('SELECT * FROM condiciones_ambientales_muestreos', conn)
        df_condicion_introducida = df_condiciones[(df_condiciones['salida']==id_salida) & (df_condiciones['estacion']==id_estacion_elegida)]               
        conn.close()
        
        if df_condicion_introducida.shape[0] == 1:
            
            texto_error = 'Ya existen datos correspondientes a la estación y salida seleccionada.'
            st.warning(texto_error, icon="⚠️") 
        
            # Asigna como valores por defecto los que ya estaban en la base de datos
            hora_llegada_defecto            = df_condicion_introducida['hora_llegada'][0]
            profundidad_defecto             = df_condicion_introducida['profundidad'][0]
            nubosidad_defecto               = df_condicion_introducida['nubosidad'][0]
            indice_lluvia_defecto           = seleccion_SN.index(df_condicion_introducida['lluvia'][0])
            velocidad_viento_defecto        = df_condicion_introducida['velocidad_viento'][0]
            indice_direccion_viento_defecto = direcciones.index(df_condicion_introducida['direccion_viento'][0])
            pres_atmosferica_defecto        = df_condicion_introducida['pres_atmosferica'][0]
            altura_ola_defecto              = df_condicion_introducida['altura_ola'][0]
            indice_mar_fondo_defecto        = seleccion_SN.index(df_condicion_introducida['mar_fondo'][0])
            indice_mar_direccion_defecto    = direcciones.index(df_condicion_introducida['mar_direccion'][0])
            temp_aire_defecto               = df_condicion_introducida['temp_aire'][0]
            indice_marea_defecto            = mareas.index(df_condicion_introducida['marea'][0])
            prof_secchi_defecto             = df_condicion_introducida['prof_secchi'][0]
            max_clorofila_defecto           = df_condicion_introducida['max_clorofila'][0]
            humedad_relativa_defecto        = df_condicion_introducida['humedad_relativa'][0]
            
            io_previo                       = 1
            
        else:
            hora_llegada_defecto            = datetime.time(8,30,0,0,tzinfo = datetime.timezone.utc)
            profundidad_defecto             = 0
            nubosidad_defecto               = 0
            indice_lluvia_defecto           = 0
            velocidad_viento_defecto        = 0
            indice_direccion_viento_defecto = 0
            pres_atmosferica_defecto        = 1022
            altura_ola_defecto              = 0
            indice_mar_fondo_defecto        = 0
            indice_mar_direccion_defecto    = 0
            temp_aire_defecto               = 15
            indice_marea_defecto            = 0
            prof_secchi_defecto             = 0
            max_clorofila_defecto           = 0
            humedad_relativa_defecto        = 50
            
            io_previo                       = 0
                       
         
        with st.form("Formulario seleccion"): 
               
            texto_estacion  = 'Estacion ' + estacion_elegida
            st.write(texto_estacion)
            
            col1, col2,col3,col4= st.columns(4,gap="small")

            with col1:
                hora_llegada  = st.time_input('Hora de llegada (UTC)',value=hora_llegada_defecto)
                profundidad   = st.number_input('Profundidad(m):',format='%i',value=profundidad_defecto,min_value=0)
                nubosidad     = st.number_input('Nubosidad(%) :',format='%i',value=nubosidad_defecto,min_value=0)
                lluvia        = st.selectbox('LLuvia:',(seleccion_SN),index=indice_lluvia_defecto)

                   
            with col2:
                velocidad_viento  = st.number_input('Vel.Viento(m/s):',value=float(velocidad_viento_defecto),min_value=float(0),step =0.5)
                direccion_viento  = st.selectbox('Dir.Viento:',(direcciones),index = indice_direccion_viento_defecto)
                pres_atmosferica  = st.number_input('Presion atm.(mmHg):',format='%i',value=pres_atmosferica_defecto,min_value=0)

                for idato_beaufort in range(len(beaufort_nombre)):
                    if velocidad_viento*3.6 >= beaufort_vmin[idato_beaufort] and velocidad_viento*3.6 < beaufort_vmax[idato_beaufort]:
                        indice_prop = idato_beaufort
      
                viento_beaufort  = st.selectbox('Viento Beaufort:',(beaufort_nombre),index=indice_prop)
                
            with col3:
                 altura_ola  = st.number_input('Altura de ola(m):',value=float(altura_ola_defecto),min_value=float(0),step =0.5)
                 # for idato_douglas in range(len(doglas_nombre)):
                 #     if altura_ola == 0:
                 #         indice_prop = 0
                 #     else:
                 #         if altura_ola > douglas_hmin[idato_douglas] and altura_ola <= douglas_hmax[idato_douglas]:
                 #             indice_prop = idato_douglas
                 # mar_douglas = st.selectbox('Mar Douglas:',(doglas_nombre),index=indice_prop)
                 mar_fondo   = st.selectbox('Mar de fondo:',(seleccion_SN),index = indice_mar_fondo_defecto)
                 mar_direccion = st.selectbox('Dir.Oleaje:',(direcciones),index = indice_mar_direccion_defecto)
                 prof_secchi   = st.number_input('Prof.Sechi(m):',value=float(prof_secchi_defecto),min_value=float(0),step=0.5)

    
            with col4:
                 temp_aire        = st.number_input('Temperatura del aire(ºC):',value=float(temp_aire_defecto),min_value=float(0),step=0.1)
                 marea            = st.selectbox('Marea:',(mareas),index = indice_marea_defecto)
                 humedad_relativa = st.number_input('Humedad relativa(%):',value=humedad_relativa_defecto,min_value=0)
                 max_clorofila    = st.number_input('Max.Clorofila(m):',value=float(max_clorofila_defecto),min_value=float(0),step=0.5)
                 
            submit = st.form_submit_button("Enviar")                    

            if submit is True:
                
                instruccion_sql = '''INSERT INTO condiciones_ambientales_muestreos (salida,estacion,hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,viento_beaufort,altura_ola,mar_fondo,mar_direccion,humedad_relativa,temp_aire,marea,prof_secchi,max_clorofila)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (salida,estacion) DO UPDATE SET (hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,viento_beaufort,altura_ola,mar_fondo,mar_direccion,humedad_relativa,temp_aire,marea,prof_secchi,max_clorofila) = ROW(EXCLUDED.hora_llegada,EXCLUDED.profundidad,EXCLUDED.nubosidad,EXCLUDED.lluvia,EXCLUDED.velocidad_viento,EXCLUDED.direccion_viento,EXCLUDED.pres_atmosferica,EXCLUDED.viento_beaufort,EXCLUDED.altura_ola,EXCLUDED.mar_fondo,EXCLUDED.mar_direccion,EXCLUDED.humedad_relativa,EXCLUDED.temp_aire,EXCLUDED.marea,EXCLUDED.prof_secchi,EXCLUDED.max_clorofila);''' 
                        
                conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()
                cursor.execute(instruccion_sql, (id_salida,id_estacion_elegida,hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,viento_beaufort,altura_ola,mar_fondo,mar_direccion,humedad_relativa,temp_aire,marea,prof_secchi,max_clorofila))
                conn.commit()
                cursor.close()
                conn.close()

                if io_previo == 0:
                    texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' añadidos correctamente'
                if io_previo == 1:
                    texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' actualizados correctamente'
                    
                st.success(texto_exito)                
                







###############################################################################
##################### PÁGINA DE ENTRADA DE DATOS DE BOTELLAS ##################
###############################################################################    

def entrada_botellas():
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['Añadir datos de botellas', 'Realizar control de calidad de datos de botellas']
    tipo_accion  = st.sidebar.radio("Indicar la acción a realizar",acciones)
    

    # Añade datos de botellas
    if tipo_accion == acciones[0]: 
        
        st.subheader('Entrada de datos procedentes de botellas') 
    
        # Recupera tablas con informacion utilizada en el procesado
        conn                = init_connection()
        df_salidas          = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas        = psql.read_sql('SELECT * FROM programas', conn)
        conn.close()    
        
        id_radiales   = df_programas.index[df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
        #id_radiales   = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].iloc[0]

        
        # Despliega menús de selección del programa, tipo de salida, año y fecha               
        col1, col2, col3= st.columns(3,gap="small")
     
        with col1: 
            programa_seleccionado     = st.selectbox('Programa',(df_programas['nombre_programa']),index=id_radiales)   
            df_salidas_seleccion      = df_salidas[df_salidas['nombre_programa']==programa_seleccionado]
            abreviatura_programa      = df_programas['abreviatura'][['nombre_programa']==programa_seleccionado]            
        
        with col2:
            tipo_salida_seleccionada  = st.selectbox('Tipo de salida',(df_salidas_seleccion['tipo_salida'].unique()))   
            df_salidas_seleccion      = df_salidas_seleccion[df_salidas_seleccion['tipo_salida']==tipo_salida_seleccionada]
        
            # Añade la variable año al dataframe
            indices_dataframe               = numpy.arange(0,df_salidas_seleccion.shape[0],1,dtype=int)    
            df_salidas_seleccion['id_temp'] = indices_dataframe
            df_salidas_seleccion.set_index('id_temp',drop=False,append=False,inplace=True)
            
            # Define los años con salidas asociadas
            df_salidas_seleccion['año'] = numpy.zeros(df_salidas_seleccion.shape[0],dtype=int)
            for idato in range(df_salidas_seleccion.shape[0]):
                df_salidas_seleccion['año'][idato] = df_salidas_seleccion['fecha_salida'][idato].year 
            df_salidas_seleccion       = df_salidas_seleccion.sort_values('fecha_salida')
            
            listado_anhos              = df_salidas_seleccion['año'].unique()
        
        with col3:
            anho_seleccionado           = st.selectbox('Año',(listado_anhos),index=len(listado_anhos)-1)
            df_salidas_seleccion        = df_salidas_seleccion[df_salidas_seleccion['año']==anho_seleccionado]
    
        salida                      = st.selectbox('Muestreo',(df_salidas_seleccion['nombre_salida']),index=df_salidas_seleccion.shape[0]-1)   
    
        # Recupera el identificador de la salida seleccionada
        id_salida                   = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==salida].iloc[0]
    
        fecha_salida                = df_salidas_seleccion['fecha_salida'][df_salidas_seleccion['nombre_salida']==salida].iloc[0]
    
    
        with st.form("my-form", clear_on_submit=True):
         
            # Despliega la extensión para subir archivos       
            listado_archivos_subidos = st.file_uploader("Arrastra o selecciona los archivos .btl", accept_multiple_files=True)
          
            # Conecta con la base de datos
            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            cursor = conn.cursor() 
            
            for archivo_subido in listado_archivos_subidos:
                
                texto_estado = 'Procesando el archivo ' + archivo_subido.name
                with st.spinner(texto_estado):
                    
                    try:
                    
                        # Lee los datos de cada archivo de botella
                        nombre_archivo = archivo_subido.name
                        datos_archivo = archivo_subido.getvalue().decode('utf-8').splitlines()            
                        
                        # Comprueba que la fecha del archivo y de la salida coinciden
                        fecha_salida_texto    = nombre_archivo[0:8]
                        fecha_salida_archivo  = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()
                        
                        if fecha_salida_archivo == fecha_salida:
                        
                            mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_INSERCION.lectura_btl(nombre_archivo,datos_archivo,programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)
                
                            # Asigna el identificador de la salida al mar
                            datos_botellas ['id_salida'] =  id_salida
                
                            # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
                            datos_botellas = FUNCIONES_INSERCION.evalua_registros(datos_botellas,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
                
                
                            # Inserta datos físicos
                            for idato in range(datos_botellas.shape[0]):
                                if io_par == 1:
                                    instruccion_sql = '''INSERT INTO datos_discretos_fisica (muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf)
                                          VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf,par_ctd,par_ctd_qf) = ROW(EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf,EXCLUDED.par_ctd,EXCLUDED.par_ctd_qf);''' 
                                    
                                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato]),datos_botellas['par_ctd'][idato],int(datos_botellas['par_ctd_qf'][idato])))
                                    conn.commit()
                                    
                                if io_par == 0:   
                                    instruccion_sql = '''INSERT INTO datos_discretos_fisica (muestreo,temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf)
                                          VALUES (%s,%s,%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (temperatura_ctd,temperatura_ctd_qf,salinidad_ctd,salinidad_ctd_qf) = ROW(EXCLUDED.temperatura_ctd,EXCLUDED.temperatura_ctd_qf,EXCLUDED.salinidad_ctd,EXCLUDED.salinidad_ctd_qf);''' 
                                            
                                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['temperatura_ctd'][idato],int(datos_botellas['temperatura_ctd_qf'][idato]),datos_botellas['salinidad_ctd'][idato],int(datos_botellas['salinidad_ctd_qf'][idato])))
                                    conn.commit()
                                    
                                # Inserta datos biogeoquímicos
                                if io_fluor == 1:                
                                    instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (muestreo,fluorescencia_ctd,fluorescencia_ctd_qf)
                                          VALUES (%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (fluorescencia_ctd,fluorescencia_ctd_qf) = ROW(EXCLUDED.fluorescencia_ctd,EXCLUDED.fluorescencia_ctd_qf);''' 
                                            
                                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['fluorescencia_ctd'][idato],int(datos_botellas['fluorescencia_ctd'][idato])))
                                    conn.commit()           
                     
                                if io_O2 == 1:                
                                    instruccion_sql = '''INSERT INTO datos_discretos_biogeoquimica (muestreo,oxigeno_ctd,oxigeno_ctd_qf)
                                          VALUES (%s,%s,%s) ON CONFLICT (muestreo) DO UPDATE SET (oxigeno_ctd,oxigeno_ctd_qf) = ROW(EXCLUDED.oxigeno_ctd,EXCLUDED.oxigeno_ctd_qf);''' 
                                            
                                    cursor.execute(instruccion_sql, (int(datos_botellas['id_muestreo_temp'][idato]),datos_botellas['oxigeno_ctd'][idato],int(datos_botellas['oxigeno_ctd_qf'][idato])))
                                    conn.commit()     
                
                            texto_exito = 'Archivo ' + archivo_subido.name + ' procesado correctamente'
                            st.success(texto_exito) 
                            
                        else:
                        
                            texto_error = 'La fecha del archivo ' + archivo_subido.name + ' no coindice con la fecha seleccionada '
                            st.warning(texto_error, icon="⚠️")                    
                            
                    
                    except:
                        texto_error = 'Error en el procesado del archivo ' + archivo_subido.name
                        st.warning(texto_error, icon="⚠️")
        
            cursor.close()
            conn.close()   
        
            st.form_submit_button("Procesar los archivos seleccionados") 
            
            

    # Control de calidad 
    if tipo_accion == acciones[1]: 
    
        st.subheader('Control de calidad de datos procedentes de botellas')    
                
        # Recupera tablas con informacion utilizada en el procesado
        conn                    = init_connection()
        df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
        df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
        df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
        df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas            = psql.read_sql('SELECT * FROM programas', conn)
        df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
        df_indices_calidad      = psql.read_sql('SELECT * FROM indices_calidad', conn)
        conn.close()    
        
        id_radiales   = df_programas.index[df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
    
        # Despliega menús de selección del programa, tipo de salida, año y fecha               
        col1, col2, col3= st.columns(3,gap="small")
     
        with col1: 
            programa_seleccionado     = st.selectbox('Programa',(df_programas['nombre_programa']),index=id_radiales)   
            df_salidas_seleccion      = df_salidas[df_salidas['nombre_programa']==programa_seleccionado]
            
        
        with col2:
            tipo_salida_seleccionada  = st.selectbox('Tipo de salida',(df_salidas_seleccion['tipo_salida'].unique()))   
            df_salidas_seleccion      = df_salidas_seleccion[df_salidas_seleccion['tipo_salida']==tipo_salida_seleccionada]
        
            # Añade la variable año al dataframe
            indices_dataframe               = numpy.arange(0,df_salidas_seleccion.shape[0],1,dtype=int)    
            df_salidas_seleccion['id_temp'] = indices_dataframe
            df_salidas_seleccion.set_index('id_temp',drop=False,append=False,inplace=True)
            
            # Define los años con salidas asociadas
            df_salidas_seleccion['año'] = numpy.zeros(df_salidas_seleccion.shape[0],dtype=int)
            for idato in range(df_salidas_seleccion.shape[0]):
                df_salidas_seleccion['año'][idato] = df_salidas_seleccion['fecha_salida'][idato].year 
            df_salidas_seleccion       = df_salidas_seleccion.sort_values('fecha_salida')
            
            listado_anhos              = df_salidas_seleccion['año'].unique()
        
        with col3:
            anho_seleccionado           = st.selectbox('Año',(listado_anhos),index=len(listado_anhos)-1)
            df_salidas_seleccion        = df_salidas_seleccion[df_salidas_seleccion['año']==anho_seleccionado]
                
        salida                      = st.selectbox('Muestreo',(df_salidas_seleccion['nombre_salida']),index=df_salidas_seleccion.shape[0]-1)   
    
        # Recupera el identificador de la salida seleccionada
        id_salida                   = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==salida].iloc[0]
        
        # Recupera los muestreos de la salida seleccionada
        df_muestreos_salida = df_muestreos[df_muestreos['salida_mar']==id_salida]  
        
        if df_muestreos_salida.shape[0] == 0:
            
            texto_error = 'No hay datos disponibles para la salida seleccionada '
            st.warning(texto_error, icon="⚠️")        
            
        else:
        
            # Determina las estaciones muestreadas en la salida selecionada
            listado_estaciones         = df_muestreos_salida['estacion'].unique()
            df_estaciones_muestreadas  = df_estaciones[df_estaciones['id_estacion'].isin(listado_estaciones)]
            nombres_estaciones         = df_estaciones_muestreadas['nombre_estacion'].tolist()
            listado_estaciones         = df_estaciones_muestreadas['id_estacion'].tolist()
            
            # Despliega menús de selección de la variable y la estación a controlar                
            col1, col2 = st.columns(2,gap="small")
         
            with col1: 
                estacion_seleccionada = st.selectbox('Estación',(nombres_estaciones))
                indice_estacion       = listado_estaciones[nombres_estaciones.index(estacion_seleccionada)]
                df_muestreos_estacion = df_muestreos_salida[df_muestreos_salida['estacion']==indice_estacion]
                listado_muestreos     = df_muestreos_estacion['id_muestreo']
            
            with col2:
                listado_variables     = ['temperatura_ctd','salinidad_ctd','par_ctd','fluorescencia_ctd','oxigeno_ctd']
                nombre_variables      = ['Temperatura','Salinidad','PAR','Fluorescencia','O2']
                uds_variables         = ['ºC','psu','\u03BCE/m2.s1','\u03BCg/kg','\u03BCmol/kg']
                variable_seleccionada = st.selectbox('Variable',(nombre_variables))
            
                indice_variable = nombre_variables.index(variable_seleccionada)
    
            if indice_variable <=2: # Datos fisicos
                df_temp         = df_datos_fisicos[df_datos_fisicos['muestreo'].isin(listado_muestreos)]
                tabla_actualiza = 'datos_discretos_fisica'
                identificador   = 'id_disc_fisica'
            else:                    # Datos biogeoquimicos
                df_temp         = df_datos_biogeoquimicos[df_datos_biogeoquimicos['muestreo'].isin(listado_muestreos)]        
                tabla_actualiza = 'datos_discretos_biogeoquimica'
                identificador   = 'id_disc_biogeoquim'
    
            # Une los dataframes con los datos del muestreo y de las variables, para tener los datos de profundidad, botella....
            df_muestreos_estacion = df_muestreos_estacion.rename(columns={"id_muestreo": "muestreo"}) # Para igualar los nombres de columnas                                               
            df_temp               = pandas.merge(df_temp, df_muestreos_estacion, on="muestreo")
                
            # Ordena los registros del dataframe por profundidades
            df_temp = df_temp.sort_values('presion_ctd',ascending=False)
            
            
            datos_variable    = df_temp[listado_variables[indice_variable]]
     
             
        
            # Representa un gráfico con la variable seleccionada
            fig, ax = plt.subplots()
            ax.plot(datos_variable,df_temp['presion_ctd'],'.k' )
            texto_eje = nombre_variables[indice_variable] + '(' + uds_variables[indice_variable] + ')'
            ax.set(xlabel=texto_eje)
            ax.set(ylabel='Presion (db)')
            ax.invert_yaxis()
            # Añade el nombre de cada punto
            nombre_muestreos = [None]*len(datos_variable)
            for ipunto in range(len(datos_variable)):
                if df_temp['botella'].iloc[ipunto] is None:
                    nombre_muestreos[ipunto] = 'Prof.' + str(df_temp['presion_ctd'].iloc[ipunto])
                else:
                    nombre_muestreos[ipunto] = 'Bot.' + str(df_temp['botella'].iloc[ipunto])
                ax.annotate(nombre_muestreos[ipunto], (datos_variable.iloc[ipunto], df_temp['presion_ctd'].iloc[ipunto]))
            
            st.pyplot(fig)
        
            #
            with st.form("Formulario", clear_on_submit=False):
                           
                indice_validacion = df_indices_calidad['indice'].tolist()
                texto_indice      = df_indices_calidad['descripcion'].tolist()
                qf_asignado       = numpy.zeros(len(datos_variable))
                
                for idato in range(len(datos_variable)):
                    
                    enunciado          = 'QF del muestreo ' + nombre_muestreos[idato]
                    valor_asignado     = st.radio(enunciado,texto_indice,horizontal=True,key = idato,index = 1)
                    qf_asignado[idato] = indice_validacion[texto_indice.index(valor_asignado)]
                
                io_envio = st.form_submit_button("Asignar los índices seleccionados")  
         
            if io_envio:
                
                texto_estado = 'Actualizando los índices de la base de datos'
                with st.spinner(texto_estado):
                
                    # Introducir los valores en la base de datos
                    conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()  
            
                    for idato in range(len(datos_variable)):
         
                        instruccion_sql = "UPDATE " + tabla_actualiza + " SET " + listado_variables[indice_variable] + '_qf = %s WHERE ' + identificador + '= %s;'
                        cursor.execute(instruccion_sql, (int(qf_asignado[idato]),int(df_temp[identificador].iloc[idato])))
                        conn.commit() 
    
                    cursor.close()
                    conn.close()   
         
                texto_exito = 'QF de la variable  ' + variable_seleccionada + ' asignados correctamente'
                st.success(texto_exito)
    
    
    





# ###############################################################################
# ##################### PÁGINA DE CONSULTA DE DATOS DE BOTELLAS #################
# ###############################################################################    


def consulta_botellas():
        
    st.subheader('Consulta los datos de botellas disponibles') 

    # Recupera tablas con informacion utilizada en el procesado
    conn                    = init_connection()
    df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    df_programas            = psql.read_sql('SELECT * FROM programas', conn)
    df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
    df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
    df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
    conn.close()    
    
    id_radiales   = df_programas.index[df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
    
    # Despliega menús de selección del programa, tipo de salida, año y fecha               
    col1, col2, col3= st.columns(3,gap="small")
 
    with col1: 
        programa_seleccionado     = st.selectbox('Programa',(df_programas['nombre_programa']),index=id_radiales)   
        df_salidas_seleccion      = df_salidas[df_salidas['nombre_programa']==programa_seleccionado]
        
    
    with col2:
        tipo_salida_seleccionada  = st.selectbox('Tipo de salida',(df_salidas_seleccion['tipo_salida'].unique()))   
        df_salidas_seleccion      = df_salidas_seleccion[df_salidas_seleccion['tipo_salida']==tipo_salida_seleccionada]
    
        indices_dataframe               = numpy.arange(0,df_salidas_seleccion.shape[0],1,dtype=int)    
        df_salidas_seleccion['id_temp'] = indices_dataframe
        df_salidas_seleccion.set_index('id_temp',drop=True,append=False,inplace=True)
        
        # Define los años con salidas asociadas
        df_salidas_seleccion['año'] = numpy.zeros(df_salidas_seleccion.shape[0],dtype=int)
        for idato in range(df_salidas_seleccion.shape[0]):
            df_salidas_seleccion['año'][idato] = df_salidas_seleccion['fecha_salida'][idato].year 
        df_salidas_seleccion       = df_salidas_seleccion.sort_values('fecha_salida')
        
        listado_anhos              = df_salidas_seleccion['año'].unique()
    
    with col3:
        anho_seleccionado           = st.selectbox('Año',(listado_anhos),index=len(listado_anhos)-1)
        df_salidas_seleccion        = df_salidas_seleccion[df_salidas_seleccion['año']==anho_seleccionado]

    # A partir del programa y año elegido, selecciona uno o varios muestreos   
    listado_salidas                 = st.multiselect('Muestreo',(df_salidas_seleccion['nombre_salida']))   
  
    if len(listado_salidas) > 0:  
  
        identificadores_salidas         = numpy.zeros(len(listado_salidas),dtype=int)
        for idato in range(len(listado_salidas)):
            identificadores_salidas[idato] = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==listado_salidas[idato]].iloc[0]
    
        # Elimina las columnas que no interesan en los dataframes a utilizar
        #df_salidas_seleccion        = df_salidas_seleccion.drop(df_salidas_seleccion.columns.difference(['id_salida']), 1, inplace=True)
        df_salidas_seleccion        = df_salidas_seleccion.drop(columns=['nombre_salida','programa','nombre_programa','tipo_salida','fecha_salida','hora_salida','fecha_retorno','hora_retorno','buque','estaciones','participantes_comisionados','participantes_no_comisionados','observaciones','año'])
        df_muestreos                = df_muestreos.drop(columns=['configuracion_perfilador','configuracion_superficie'])
        df_datos_biogeoquimicos     = df_datos_biogeoquimicos.drop(columns=['r_clor','r_clor_qf','r_per','r_per_qf','co3_temp'])
    
        # conserva los datos de las salidas seleccionadas
        df_salidas_seleccion = df_salidas_seleccion[df_salidas_seleccion['id_salida'].isin(identificadores_salidas)]
    
        # Recupera los muestreos correspondientes a las salidas seleccionadas
        df_muestreos                = df_muestreos.rename(columns={"salida_mar": "id_salida"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_salidas_seleccion, df_muestreos, on="id_salida")
                              
        # Asocia las coordenadas y nombre de estación de cada muestreo
        df_estaciones               = df_estaciones.rename(columns={"id_estacion": "estacion"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_estaciones, on="estacion")
        
        # Asocia las propiedades físicas de cada muestreo
        df_datos_fisicos            = df_datos_fisicos.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_datos_fisicos, on="id_muestreo")
    
        # Asocia las propiedades biogeoquimicas de cada muestreo
        df_datos_biogeoquimicos     = df_datos_biogeoquimicos.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_datos_biogeoquimicos, on="id_muestreo")
    
        # Elimina las columnas que no interesan
        df_exporta                  = df_muestreos_seleccionados.drop(columns=['id_salida','estacion','programa','profundidades_referencia'])
    
        # Mueve os identificadores de muestreo al final del dataframe
        listado_cols = df_exporta.columns.tolist()
        listado_cols.append(listado_cols.pop(listado_cols.index('id_muestreo')))
        listado_cols.append(listado_cols.pop(listado_cols.index('id_disc_fisica')))
        listado_cols.append(listado_cols.pop(listado_cols.index('id_disc_biogeoquim')))
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('longitud')))    
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('latitud')))
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('nombre_estacion')))
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('nombre_muestreo')))
        df_exporta = df_exporta[listado_cols]
    
        # Ordena los valores por fechas
        df_exporta = df_exporta.sort_values('fecha_muestreo')
    
        ## Botón para exportar los resultados
        nombre_archivo =  'DATOS_BOTELLAS.xlsx'
    
        output = BytesIO()
        writer = pandas.ExcelWriter(output, engine='xlsxwriter')
        df_exporta.to_excel(writer, index=False, sheet_name='DATOS')
        workbook = writer.book
        worksheet = writer.sheets['DATOS']
        writer.save()
        datos_exporta = output.getvalue()
    
        st.download_button(
            label="DESCARGA LOS DATOS DISPONIBLES DE LOS MUESTREOS SELECCIONADOS",
            data=datos_exporta,
            file_name=nombre_archivo,
            help= 'Descarga un archivo .csv con los datos solicitados',
            mime="application/vnd.ms-excel"
        )
        
        
        
        
###############################################################################
##################### PÁGINA DE CONSULTA DE DATOS DE BOTELLAS #################
###############################################################################    


def procesado_nutrientes():
        
    st.subheader('Procesado de datos de nutrientes')
    
    # Recupera los datos de conexión
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port
    
    # Recupera tablas con informacion utilizada en el procesado
    conn                    = init_connection()
    df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
    df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
    df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
    df_indices_calidad      = psql.read_sql('SELECT * FROM indices_calidad', conn)
    conn.close()     
    
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['Procesar salidas del AA', 'Realizar control de calidad de datos disponibles']
    tipo_accion  = st.sidebar.radio("Indicar la acción a realizar",acciones)
    

    # Añade salidas del AA
    if tipo_accion == acciones[0]:
        
        
        variables_run = ['nitrogeno_total','nitrito','silicato','fosfato']    
    
    
        # Despliega un formulario para subir los archivos del AA y las referencias
        temperatura_laboratorio  = st.number_input('Temperatura laboratorio:',value=20)
        archivo_AA               = st.file_uploader("Arrastra o selecciona los archivos del AA", accept_multiple_files=False)
        archivo_refs             = st.file_uploader("Arrastra o selecciona los archivos con las referencias", accept_multiple_files=False)
            
        if archivo_AA is not None and archivo_refs is not None:
    
            # Lectura del archivo con las referencias
            df_referencias        = pandas.read_excel(archivo_refs)   
        
            # Lectura del archivo con los resultados del AA
            datos_brutos=pandas.read_excel(archivo_AA,skiprows=15)
            
            # Cambia los nombres de cada variable analizada
            datos_AA      = datos_brutos.rename(columns={"Results 1":variables_run[0],"Results 2":variables_run[1],"Results 3":variables_run[2],"Results 4":variables_run[3]})
        
            # Predimensiona columnas en las que guardar información de salinidad y densidad    
            datos_AA['densidad']    = numpy.ones(datos_AA.shape[0])
            
            # Genera un dataframe en el que se almacenarán los resultados de las correcciones aplicadas. 
            datos_corregidos    = pandas.DataFrame(columns=variables_run)
            # Añade columnas con variables a utilizar en el control de calidad posterior 
            datos_corregidos['muestreo']        = [None]*datos_AA.shape[0]
            datos_corregidos['presion_ctd']     = [None]*datos_AA.shape[0]
#            datos_corregidos['ph']              = [None]*datos_AA.shape[0]
            datos_corregidos['ph']              = None
            datos_corregidos['alcalinidad']     = [None]*datos_AA.shape[0]
            datos_corregidos['oxigeno_ctd']     = [None]*datos_AA.shape[0]  
            datos_corregidos['oxigeno_wk']      = [None]*datos_AA.shape[0] 
            datos_corregidos['estacion']        = numpy.zeros(datos_AA.shape[0],dtype=int)
            datos_corregidos['salida_mar']      = numpy.zeros(datos_AA.shape[0],dtype=int)
            datos_corregidos['botella']         = numpy.zeros(datos_AA.shape[0],dtype=int)
            datos_corregidos['id_disc_biogeoquim'] = numpy.zeros(datos_AA.shape[0],dtype=int)
            datos_corregidos['fecha_muestreo']  = [None]*datos_AA.shape[0] 
        
            # Busca los datos de cada tubo analizada en el AA
            for idato in range(datos_AA.shape[0]):
                
                if datos_AA['Sample ID'].iloc[idato] == 'RMN Low' : # Tubo correspondiente a referencia (RMN)
                    datos_AA['densidad'].iloc[idato]  = (999.1+0.77*((df_referencias['Sal'][0])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
                    
                elif datos_AA['Sample ID'].iloc[idato] == 'RMN High': # Tubo correspondiente a referencia (RMN)
                    datos_AA['densidad'].iloc[idato]  = (999.1+0.77*((df_referencias['Sal'][1])-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
                
                else:   # Resto de tubos
                    id_temp = df_muestreos['id_muestreo'][df_muestreos['nombre_muestreo']==datos_AA['Sample ID'].iloc[idato]]
                    
                    if len(id_temp) > 0:
                        indice                                             = id_temp.iloc[0]
                        salinidad                                          = df_datos_fisicos['salinidad_ctd'][df_datos_fisicos['muestreo']==indice]
        
                        datos_corregidos['muestreo'].iloc[idato]           = indice
                        datos_corregidos['presion_ctd'].iloc[idato]        = df_muestreos['presion_ctd'][df_muestreos['id_muestreo']==indice]
                        datos_corregidos['salida_mar'].iloc[idato]         = df_muestreos['salida_mar'][df_muestreos['id_muestreo']==indice]
                        datos_corregidos['botella'].iloc[idato]            = df_muestreos['botella'][df_muestreos['id_muestreo']==indice]
                        datos_corregidos['fecha_muestreo'].iloc[idato]     = df_muestreos['fecha_muestreo'][df_muestreos['id_muestreo']==indice]
                                            
                        datos_corregidos['id_disc_biogeoquim'].iloc[idato] = df_datos_biogeoquimicos['id_disc_biogeoquim'][df_datos_biogeoquimicos['muestreo']==indice]
                        datos_corregidos['ph'].iloc[idato]                 = df_datos_biogeoquimicos['ph'][df_datos_biogeoquimicos['muestreo']==indice]                              
                        datos_corregidos['alcalinidad'].iloc[idato]        = df_datos_biogeoquimicos['alcalinidad'][df_datos_biogeoquimicos['muestreo']==indice]
                        datos_corregidos['oxigeno_ctd'].iloc[idato]        = df_datos_biogeoquimicos['oxigeno_ctd'][df_datos_biogeoquimicos['muestreo']==indice]
                        datos_corregidos['oxigeno_wk'].iloc[idato]         = df_datos_biogeoquimicos['oxigeno_wk'][df_datos_biogeoquimicos['muestreo']==indice]
                            
                        datos_corregidos['estacion'].iloc[idato]           =  df_muestreos['estacion'][df_muestreos['id_muestreo']==indice]
                    
                        datos_AA['densidad'].iloc[idato]    = (999.1+0.77*((salinidad)-((temperatura_laboratorio-15)/5.13)-((temperatura_laboratorio-15)**2)/128))/1000
                       
            # Asigna el identificador de cada registro al dataframe en el que se guardarán los resultados
            datos_corregidos['tubo'] = datos_AA['Sample ID']
                                 
            # Aplica la corrección de drift de cada variable
            for ivariable in range(len(variables_run)):
        
                valores_brutos = datos_AA[variables_run[ivariable]] # Selecciona la variable y convierte a concentraciones
                densidades     = datos_AA['densidad']
                
                valores_concentraciones = valores_brutos / densidades
                
                # Concentraciones de las referencias
                RMN_CE_variable = df_referencias[variables_run[ivariable]][0] 
                RMN_CI_variable = df_referencias[variables_run[ivariable]][1]     
        
                # Encuentra las posiciones de los RMNs
                posicion_RMN_bajos  = [i for i, e in enumerate(datos_AA['Sample ID']) if e == 'RMN Low']
                posicion_RMN_altos  = [i for i, e in enumerate(datos_AA['Sample ID']) if e == 'RMN High']
        
                # Predimensiona las rectas a y b
                posiciones_corr_drift = numpy.arange(posicion_RMN_altos[0],posicion_RMN_bajos[1])
                recta_at              = numpy.zeros(datos_AA.shape[0])
                recta_bt              = numpy.zeros(datos_AA.shape[0])
        
                RMN_altos = valores_concentraciones[posicion_RMN_altos]
                RMN_bajos = valores_concentraciones[posicion_RMN_bajos]
        
                pte_RMN      = (RMN_CI_variable-RMN_CE_variable)/(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) 
                t_indep_RMN  = RMN_CE_variable- pte_RMN*RMN_bajos.iloc[0] 
        
                variable_drift = numpy.zeros(datos_AA.shape[0])
        
                # Aplica la corrección basada de dos rectas, descrita en Hassenmueller
                for idato in range(posiciones_corr_drift[0],posiciones_corr_drift[-1]):
                    
                
                    factor_f        = (idato-posiciones_corr_drift[0])/(posiciones_corr_drift[-1]-posiciones_corr_drift[0])
                    recta_at[idato] = RMN_bajos.iloc[0] +  factor_f*(RMN_bajos.iloc[0]-RMN_bajos.iloc[-1]) 
                    recta_bt[idato] = RMN_altos.iloc[0] -  factor_f*(RMN_altos.iloc[0]-RMN_altos.iloc[-1]) 
                    
                    val_combinado         = ((valores_concentraciones[idato]-recta_at[idato])/(recta_bt[idato]-recta_at[idato]))*(RMN_altos.iloc[0]-RMN_bajos.iloc[0]) + RMN_bajos.iloc[0]
        
                    variable_drift[idato] = val_combinado*pte_RMN+t_indep_RMN
        
                variable_drift[variable_drift<0] = 0
        
                # Almacena los resultados en un dataframe    
                datos_corregidos[variables_run[ivariable]] = variable_drift
         
            # # Calcula el NO3 como diferencia entre el TON y el NO2
            datos_corregidos['nitrato'] = numpy.zeros(datos_corregidos.shape[0])
            for idato in range(datos_corregidos.shape[0]):
                datos_corregidos['nitrato'].iloc[idato] = datos_corregidos['nitrogeno_total'].iloc[idato] - datos_corregidos['nitrito'].iloc[idato]
            
            if datos_corregidos['muestreo'].isnull().all():
                texto_error = "Ninguna de las muestras analizadas se corresponde con muestreos incluidos en la base de datos"
                st.warning(texto_error, icon="⚠️") 
                
            else:
            
                texto_exito = 'Datos del Autoanalizador procesados correctamente'
                st.success(texto_exito)
            
                # Mantén sólo las filas del dataframe con valores no nulos
                datos_muestras = datos_corregidos[datos_corregidos['muestreo'].isnull() == False]
             
                variables_procesado = ['nitrogeno_total','nitrato','nitrito','silicato','fosfato']  

                datos_muestras = datos_muestras.replace('None', numpy.nan)

                FUNCIONES_INSERCION.control_calidad_nutrientes(datos_muestras,variables_procesado,direccion_host,base_datos,usuario,contrasena,puerto)

        
    # control de calidad de salidas previamente disponibles
    if tipo_accion == acciones[1]: 
 
        nombres_salidas            = df_salidas['nombre_salida'].tolist()
        listado_salidas            = df_salidas['id_salida'].tolist()       
 
    #     # Despliega menús de selección de la variable, salida y la estación a controlar                
    #     col1, col2 = st.columns(2,gap="small")
    #     with col1: 
            
    #         salida_seleccionada   = st.selectbox('Salida',(nombres_salidas))
    #         indice_salida         = listado_salidas[nombres_salidas.index(salida_seleccionada)]
    
    #         variable_seleccionada  = st.selectbox('Variable',(listado_variables))
    #         indice_variable        = listado_variables.index(variable_seleccionada)
            
    #         df_muestreos_seleccionados = df_muestreos[df_muestreos['salida_mar']==indice_salida]
       
    #     with col2:
            
    #         listado_estaciones         = df_muestreos_seleccionados['estacion'].unique()
    #         df_estaciones_muestreadas  = df_estaciones[df_estaciones['id_estacion'].isin(listado_estaciones)]
                
    #         estacion_seleccionada      = st.selectbox('Estación',(df_estaciones_muestreadas['nombre_estacion']))
    #         indice_estacion            = df_estaciones['id_estacion'][df_estaciones['id_estacion'].isin(listado_estaciones)]
            
    #         df_datos_estacion          = 
            
    #         meses_offset           = st.number_input('Intervalo meses:',value=1)
            
    #     FUNCIONES_INSERCION.control_calidad_nutrientes(datos_muestras,df_salidas_muestreadas,listado_variables,listado_variables_bd,df_estaciones_muestreadas,direccion_host,base_datos,usuario,contrasena,puerto)

    
    # df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    # df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    # df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
    # df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
    # df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
    # df_indices_calidad      = psql.read_sql('SELECT * FROM indices_calidad', conn)    
 
    
    #             # Mantén sólo las filas del dataframe con valores no nulos
    #             datos_muestras = datos_corregidos[datos_corregidos['muestreo'].isnull() == False]
             
    #             listado_salidas            = datos_muestras['id_salida'].unique()
    #             df_salidas_muestreadas     = df_salidas[df_salidas['id_salida'].isin(listado_salidas)]
            
    #             listado_estaciones         = datos_muestras['id_estacion'].unique()
    #             df_estaciones_muestreadas  = df_estaciones[df_estaciones['id_estacion'].isin(listado_estaciones)]
                
    #             listado_variables      = ['NITRATO','NITRITO','SILICATO','FOSFATO']
    #             listado_variables_bd   = ['no3','no2','sio2','po4']
            
    #             FUNCIONES_INSERCION.control_calidad_nutrientes(datos_muestras,df_salidas_muestreadas,listado_variables,listado_variables_bd,df_estaciones_muestreadas,direccion_host,base_datos,usuario,contrasena,puerto)
     

        
            # ### CONTROL DE CALIDAD DE LOS DATOS
        
            # # Determina las estaciones muestreadas en la salida selecionada
            # listado_estaciones         = datos_muestras['id_estacion'].unique()
            # df_estaciones_muestreadas  = df_estaciones[df_estaciones['id_estacion'].isin(listado_estaciones)]
            # nombres_estaciones         = df_estaciones_muestreadas['nombre_estacion'].tolist()
            # listado_estaciones         = df_estaciones_muestreadas['id_estacion'].tolist()
           
            # # Determina los muestreos realizados  
            # listado_salidas            = datos_corregidos['id_salida'].unique()
            # df_salidas_muestreadas     = df_salidas[df_salidas['id_salida'].isin(listado_salidas)]
            # nombres_salidas            = df_salidas_muestreadas['nombre_salida'].tolist()
            # listado_salidas            = df_salidas_muestreadas['id_salida'].tolist()
            
            # # Despliega menús de selección de la variable, salida y la estación a controlar                
            # col1, col2 = st.columns(2,gap="small")
            # with col1: 
                
            #     salida_seleccionada   = st.selectbox('Salida',(nombres_salidas))
            #     indice_salida         = listado_salidas[nombres_salidas.index(salida_seleccionada)]
    
            #     listado_variables      = ['NITRATO','NITRITO','SILICATO','FOSFATO']
            #     listado_variables_bd   = ['no3','no2','sio2','po4']
            #     variable_seleccionada  = st.selectbox('Variable',(listado_variables))
            #     indice_variable        = listado_variables.index(variable_seleccionada)
                
    
           
            # with col2:
            #     estacion_seleccionada = st.selectbox('Estación',(nombres_estaciones))
            #     indice_estacion       = listado_estaciones[nombres_estaciones.index(estacion_seleccionada)]
                
            #     meses_offset           = st.number_input('Intervalo meses:',value=1)
            
            # # Selecciona los datos correspondientes a la estación y salida seleccionada
            # df_seleccion               = datos_muestras[(datos_muestras["id_estacion"] == indice_estacion) & (datos_muestras["id_salida"] == indice_salida)]
            
    
    
    
    
    
    
    
            # # Recupera los datos disponibles de la misma estación, para la misma variable
            # listado_muestreos_estacion = df_muestreos['id_muestreo'][df_muestreos['estacion']==indice_estacion]
            # df_disponible_bd           = df_datos_biogeoquimicos[df_datos_biogeoquimicos['muestreo'].isin(listado_muestreos_estacion)]
            
            # df_disponible_bd            = df_disponible_bd.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
            # df_disponible_bd            = pandas.merge(df_muestreos, df_disponible_bd, on="id_muestreo")
           
            # # Determina los meses que marcan el rango de busqueda
            # df_seleccion    = df_seleccion.sort_values('fecha_muestreo')
            # fecha_minima    = df_seleccion['fecha_muestreo'].iloc[0][0] - datetime.timedelta(days=meses_offset*30)
            # fecha_maxima    = df_seleccion['fecha_muestreo'].iloc[-1][0] + datetime.timedelta(days=meses_offset*30)  
    
            # if fecha_minima.year < fecha_maxima.year:
            #     listado_meses_1 = numpy.arange(fecha_minima.month,13)
            #     listado_meses_2 = numpy.arange(1,fecha_maxima.month+1)
            #     listado_meses   = numpy.concatenate((listado_meses_1,listado_meses_2))
            
            # else:
            #     listado_meses   = numpy.arange(fecha_minima.month,fecha_maxima.month+1)
     
            # listado_meses = listado_meses.tolist()
       
            
            # # Busca los datos de la base de datos dentro del rango de meses seleccionados
            # df_disponible_bd['io_fecha'] = numpy.zeros(df_disponible_bd.shape[0],dtype=int)
            # for idato in range(df_disponible_bd.shape[0]):
            #     if (df_disponible_bd['fecha_muestreo'].iloc[idato]).month in listado_meses:
            #         df_disponible_bd['io_fecha'].iloc[idato] = 1
                    
            # df_rango_temporal = df_disponible_bd[df_disponible_bd['io_fecha']==1]
    
            # ################# GRAFICOS ################
    
            # # Representa un gráfico con la variable seleccionada junto a los oxígenos
            # fig, (ax, az) = plt.subplots(1, 2, gridspec_kw = {'wspace':0.05, 'hspace':0}, width_ratios=[3, 1])
       
            # ax.plot(df_disponible_bd[listado_variables_bd[indice_variable]],df_disponible_bd['presion_ctd'],'.',color='#C0C0C0')
            # ax.plot(df_rango_temporal[listado_variables_bd[indice_variable]],df_rango_temporal['presion_ctd'],'.',color='#404040')
            # ax.plot(df_seleccion[variable_seleccionada],df_seleccion['presion_ctd'],'.r' )
            # texto_eje = variable_seleccionada + '(\u03BCmol/kg)'
            # ax.set(xlabel=texto_eje)
            # ax.set(ylabel='Presion (db)')
            # ax.invert_yaxis()
            # rango_profs = ax.get_ylim()
            # # Añade el nombre de cada punto
            # nombre_muestreos = [None]*df_seleccion.shape[0]
            # for ipunto in range(df_seleccion.shape[0]):
            #     if df_seleccion['id_botella'].iloc[ipunto] is None:
            #         nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
            #     else:
            #         nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['id_botella'].iloc[ipunto])
            #     ax.annotate(nombre_muestreos[ipunto], (df_seleccion[variable_seleccionada].iloc[ipunto], df_seleccion['presion_ctd'].iloc[ipunto]))
           
            # az.plot(df_seleccion['Oxigeno'],df_seleccion['presion_ctd'],'.',color='#006633')
            # az.set(xlabel='Oxigeno (\u03BCmol/kg)')
            # az.yaxis.set_visible(False)
            # az.invert_yaxis()
            # az.set_ylim(rango_profs)
    
            # st.pyplot(fig)
            
            # # Gráficos particulares para cada variable
            # if variable_seleccionada == 'FOSFATO':
    
            #     fig, ax = plt.subplots()       
            #     ax.plot(df_disponible_bd['no3'],df_disponible_bd['po4'],'.',color='#C0C0C0')
            #     ax.plot(df_rango_temporal['no3'],df_rango_temporal['po4'],'.',color='#404040')
            #     ax.plot(df_seleccion['NITRATO'],df_seleccion['FOSFATO'],'.r' )
            #     ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            #     ax.set(ylabel='Fosfato (\u03BCmol/kg)')
    
            #     # Añade el nombre de cada punto
            #     nombre_muestreos = [None]*df_seleccion.shape[0]
            #     for ipunto in range(df_seleccion.shape[0]):
            #         if df_seleccion['id_botella'].iloc[ipunto] is None:
            #             nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
            #         else:
            #             nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['id_botella'].iloc[ipunto])
            #         ax.annotate(nombre_muestreos[ipunto], (df_seleccion['NITRATO'].iloc[ipunto], df_seleccion['FOSFATO'].iloc[ipunto]))
               
            #     st.pyplot(fig)
            
            # elif variable_seleccionada == 'NITRATO':
    
            #     fig, (ax, az) = plt.subplots(1, 2, gridspec_kw = {'wspace':0.1, 'hspace':0}, width_ratios=[1, 1])      
            #     ax.plot(df_disponible_bd['no3'],df_disponible_bd['po4'],'.',color='#C0C0C0')
            #     ax.plot(df_rango_temporal['no3'],df_rango_temporal['po4'],'.',color='#404040')
            #     ax.plot(df_seleccion['NITRATO'],df_seleccion['FOSFATO'],'.r' )
            #     ax.set(xlabel='Nitrato (\u03BCmol/kg)')
            #     ax.set(ylabel='Fosfato (\u03BCmol/kg)')
    
            #     # Añade el nombre de cada punto
            #     nombre_muestreos = [None]*df_seleccion.shape[0]
            #     for ipunto in range(df_seleccion.shape[0]):
            #         if df_seleccion['id_botella'].iloc[ipunto] is None:
            #             nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
            #         else:
            #             nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['id_botella'].iloc[ipunto])
            #         ax.annotate(nombre_muestreos[ipunto], (df_seleccion['NITRATO'].iloc[ipunto], df_seleccion['FOSFATO'].iloc[ipunto]))
    
            #     az.plot(df_disponible_bd['no3'],df_disponible_bd['phts25p0_unpur'],'.',color='#C0C0C0')
            #     az.plot(df_rango_temporal['no3'],df_rango_temporal['phts25p0_unpur'],'.',color='#404040')
            #     az.plot(df_disponible_bd['no3'],df_disponible_bd['phts25p0_pur'],'.',color='#C0C0C0')
            #     az.plot(df_rango_temporal['no3'],df_rango_temporal['phts25p0_pur'],'.',color='#404040')
            #     az.plot(df_seleccion['NITRATO'],df_seleccion['pH'],'.r' )
            #     az.set(xlabel='Nitrato (\u03BCmol/kg)')
            #     az.set(ylabel='pH')
            #     az.yaxis.tick_right()
            #     az.yaxis.set_label_position("right")
    
            #     # Añade el nombre de cada punto
            #     nombre_muestreos = [None]*df_seleccion.shape[0]
            #     for ipunto in range(df_seleccion.shape[0]):
            #         if df_seleccion['id_botella'].iloc[ipunto] is None:
            #             nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
            #         else:
            #             nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['id_botella'].iloc[ipunto])
            #         az.annotate(nombre_muestreos[ipunto], (df_seleccion['NITRATO'].iloc[ipunto], df_seleccion['pH'].iloc[ipunto]))
         
    
            #     st.pyplot(fig)
      
            
            # # Gráficos particulares para cada variable
            # elif variable_seleccionada == 'SILICATO':
    
            #     fig, ax = plt.subplots()       
            #     ax.plot(df_disponible_bd['sio2'],df_disponible_bd['alkali'],'.',color='#C0C0C0')
            #     ax.plot(df_rango_temporal['sio2'],df_rango_temporal['alkali'],'.',color='#404040')
            #     ax.plot(df_seleccion['SILICATO'],df_seleccion['Alcalinidad'],'.r' )
            #     ax.set(xlabel='Silicato (\u03BCmol/kg)')
            #     ax.set(ylabel='Alcalinidad (\u03BCmol/kg)')
    
            #     # Añade el nombre de cada punto
            #     nombre_muestreos = [None]*df_seleccion.shape[0]
            #     for ipunto in range(df_seleccion.shape[0]):
            #         if df_seleccion['id_botella'].iloc[ipunto] is None:
            #             nombre_muestreos[ipunto] = 'Prof.' + str(df_seleccion['presion_ctd'].iloc[ipunto])
            #         else:
            #             nombre_muestreos[ipunto] = 'Bot.' + str(df_seleccion['id_botella'].iloc[ipunto])
            #         ax.annotate(nombre_muestreos[ipunto], (df_seleccion['SILICATO'].iloc[ipunto], df_seleccion['Alcalinidad'].iloc[ipunto]))
               
            #     st.pyplot(fig)
            
            
            # ################# FORMULARIOS CALIDAD ################        
    
            # # Formulario para asignar banderas de calidad
            # with st.form("Formulario", clear_on_submit=False):
                          
            #     indice_validacion = df_indices_calidad['indice'].tolist()
            #     texto_indice      = df_indices_calidad['descripcion'].tolist()
            #     qf_asignado       = numpy.zeros(df_seleccion.shape[0])
               
            #     for idato in range(df_seleccion.shape[0]):
                   
            #         enunciado          = 'QF del muestreo ' + nombre_muestreos[idato]
            #         valor_asignado     = st.radio(enunciado,texto_indice,horizontal=True,key = idato,index = 1)
            #         qf_asignado[idato] = indice_validacion[texto_indice.index(valor_asignado)]
               
            #     io_envio = st.form_submit_button("Añadir resultados a la base de datos con los índices seleccionados")  
        
            # if io_envio:
    
            #     with st.spinner('Actualizando la base de datos'):
               
            #         # Introducir los valores en la base de datos
            #         conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
            #         cursor = conn.cursor()  
           
            #         for idato in range(df_seleccion.shape[0]):
        
            #             instruccion_sql = "UPDATE datos_discretos_biogeoquimica SET " + listado_variables_bd[indice_variable] + ' = %s, ' + listado_variables_bd[indice_variable] +  '_qf = %s WHERE id_disc_biogeoquim = %s;'
            #             cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(qf_asignado[idato]),int(df_seleccion['id_muestreo_bgq'].iloc[idato])))
            #             conn.commit() 
    
            #         cursor.close()
            #         conn.close()   
        
            #     texto_exito = 'Datos de ' + variable_seleccionada + ' correspondientes a la salida ' + salida_seleccionada + ' añadidos correctamente'
            #     st.success(texto_exito)
       
       

            
            
            
            
            
            
            
            