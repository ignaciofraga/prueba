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

#import FUNCIONES_INSERCION
from FUNCIONES import FUNCIONES_INSERCION
from FUNCIONES.FUNCIONES_AUXILIARES import init_connection


# from pages.COMUNES import FUNCIONES_AUXILIARES

pandas.options.mode.chained_assignment = None


###############################################################################
####################### PÁGINA PRINCIPAL ######################################
###############################################################################


def principal(logo_IEO_principal):

    st.title("Servicio de información de nutrientes del C.O de A Coruña")

    # Añade el logo del IEO
    imagen_pagina = Image.open(logo_IEO_principal) 
    st.image(imagen_pagina)





###############################################################################
################## PÁGINA DE ENTRADA DE DATOS #################################
###############################################################################


def entrada_datos(archivo_plantilla,archivo_instrucciones):
    
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
            btn = st.download_button(
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
        st.form_submit_button("Enviar")
    
    
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
                        estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_analisis_laboratorio'][ianho] 
                        estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_analisis_laboratorio'][ianho].strftime("%m/%d/%Y")
                    else:
                        if tiempo_consulta >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
                            estado_procesos_programa['id_estado'][ianho] = 1 
                            estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_entrada_datos'][ianho]
                            estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_entrada_datos'][ianho].strftime("%m/%d/%Y")
                                                
                
                else:
                    # Caso 1. Fecha de consulta posterior a terminar la campaña pero anterior al análisis en laboratorio, o análisis no disponible. 
                    if pandas.isnull(estado_procesos_programa['fecha_entrada_datos'][ianho]) is False:
                        if tiempo_consulta >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
                            estado_procesos_programa['id_estado'][ianho] = 1 
                            estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_entrada_datos'][ianho]
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


