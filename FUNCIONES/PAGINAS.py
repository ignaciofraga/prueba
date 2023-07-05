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
from io import BytesIO
import numpy
import psycopg2
from PIL import Image
from dateutil.relativedelta import relativedelta
import json
import time
#from random import randint

#import FUNCIONES_INSERCION
from FUNCIONES import FUNCIONES_LECTURA
from FUNCIONES import FUNCIONES_PROCESADO
from FUNCIONES import FUNCIONES_AUXILIARES
from FUNCIONES.FUNCIONES_AUXILIARES import init_connection
from FUNCIONES.FUNCIONES_AUXILIARES import estado_procesos

# from pages.COMUNES import FUNCIONES_AUXILIARES

pandas.options.mode.chained_assignment = None


###############################################################################
################### PÁGINA PRINCIPAL BIOGEOQUIMICA ############################
###############################################################################


def principal():

    logo_IEO_principal    = 'DATOS/IMAGENES/logo-CSIC.jpg'    

    st.title("Plataforma de datos biogeoquímicos del C.O de A Coruña")

    # Añade el logo del IEO
    imagen_pagina = Image.open(logo_IEO_principal) 
    st.image(imagen_pagina)
    
    st.cache_data.clear()


###############################################################################
#################### PÁGINA PRINCIPAL RADIALES ################################
###############################################################################


def principal_radiales():

    foto_lura    = 'DATOS/IMAGENES/LURA.jpg'    

    st.title("Plataforma de datos del programa RADIALES - Coruña")

    # Añade el logo del IEO
    imagen_pagina = Image.open(foto_lura) 
    st.image(imagen_pagina)
    st.caption('Fotografía de Mar Nieto Cid')
    
    st.cache_data.clear()










        
     
        
     
        
     
        
     
        
     
        
     
###############################################################################
############### PÁGINA DE CONSULTA DEL ESTADO DE LOS PROCESOS #################
###############################################################################

def consulta_estado():
        
             
    ### Encabezados y titulos 
    #st.set_page_config(page_title='CONSULTA DATOS', layout="wide",page_icon=logo_IEO_reducido) 
    st.title('Servicio de consulta de información disponible del C.O de A Coruña')
    
    # Despliega un botón lateral para seleccionar el tipo de información a introducir       
    entradas     = ['Estado del procesado de programas', 'Evolución en el procesado de programas']
    tipo_entrada = st.sidebar.radio("Indicar la consulta a realizar",entradas)

    # Vector con nombre de los procesos y colores asociados
    nombre_estados  = ['No disponible','Pendiente de análisis','Analizado','Control de calidad secundario']
    colores_estados = ['#CD5C5C','#F4A460','#87CEEB','#66CDAA','#2E8B57']  

    # Consulta del estado del procesado    
    if tipo_entrada == entradas[0]:
    
        # Recupera la tabla de los programas disponibles como un dataframe
        conn = init_connection()
        df_programas = psql.read_sql('SELECT * FROM programas', conn)
        conn.close()
        
        # Despliega un formulario para elegir el programa y la fecha a consultar
        with st.form("Formulario seleccion"):
            col1, col2 = st.columns(2,gap="small")
            with col1:
                nombre_programa  = st.selectbox('Selecciona el programa del cual se quiere consultar el estado',(df_programas['nombre_programa']))
            with col2:
                fecha_consulta = st.date_input("Selecciona fecha de consulta",datetime.date.today())
        
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
                        
                df_estados = FUNCIONES_AUXILIARES.comprueba_estado(nombre_programa,fecha_consulta,nombre_estados,temporal_estado_procesos)
                        
                df_estados = df_estados.sort_values('Año')
            
                # Despliega la información en una tabla
                def color_tabla(s):
                    if s.Estado == 'No disponible':
                        return ['background-color: #CD5C5C']*len(s)
                    elif s.Estado == 'Pendiente de análisis':
                        return ['background-color:#F4A460']*len(s)
                    elif s.Estado == 'Analizado':
                        return ['background-color:#87CEEB']*len(s)
                    elif s.Estado == 'Control de calidad secundario':                    
                        return ['background-color:#66CDAA']*len(s)
    
                st.dataframe(df_estados.style.apply(color_tabla, axis=1),use_container_width=True)    
                
                    
                # Cuenta el numero de veces que se repite cada estado para sacar un gráfico pie-chart
                num_valores = numpy.zeros(len(nombre_estados),dtype=int)
                for ivalor in range(len(nombre_estados)):
                    try:
                        num_valores[ivalor] = df_estados.Estado.value_counts()[nombre_estados[ivalor]]
                    except:
                        pass
                porcentajes = numpy.round((100*(num_valores/numpy.sum(num_valores))),0)
                
                # Construye el gráfico
                cm              = 1/2.54 # pulgadas a cm
                fig, ax1 = plt.subplots(figsize=(8*cm, 8*cm))
                patches, texts= ax1.pie(num_valores, colors=colores_estados,shadow=True, startangle=90,radius=1.2)
                ax1.axis('equal')  # Para representar el pie-chart como un circulo
                
                # Representa y ordena la leyenda
                etiquetas_leyenda = ['{0} - {1:1.0f} %'.format(i,j) for i,j in zip(nombre_estados, porcentajes)]
                plt.legend(patches, etiquetas_leyenda, loc='lower center', bbox_to_anchor=(1.5, 0.5),fontsize=8)                

                # Representa el pie-chart con el estado de los procesos
                buf = BytesIO()
                fig.savefig(buf, format="png",bbox_inches='tight')
                st.image(buf)                


        
    # Consulta del estado del procesado    
    if tipo_entrada == entradas[1]:
            
        listado_meses = numpy.arange(2,12,dtype=int)
        
        # Recupera la tabla de los programas disponibles como un dataframe
        conn = init_connection()
        df_programas       = psql.read_sql('SELECT * FROM programas', conn)
        df_estado_procesos = psql.read_sql('SELECT * FROM estado_procesos', conn)
        conn.close()
        
    
        # Despliega un formulario para elegir el programa y la fecha a consultar
        with st.form("Formulario seleccion"):
            col1, col2 = st.columns(2,gap="small")
            #nombre_programa, tiempo_consulta = st.columns((1, 1))
            with col1:
                tiempo_final_consulta = st.date_input("Selecciona la fecha de finalización del periodo de consulta",datetime.date.today())
            with col2:
                num_meses_previos     = st.selectbox("Selecciona el número de meses del periodo de consulta",listado_meses,index=4)
      
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
                
               
            # Predimensiona contador
            num_valores = numpy.zeros((len(fechas_comparacion),df_programas.shape[0],len(nombre_estados)),dtype=int)
            
            # Bucle para determinar el estado de cada programa, en cada fecha
            for iprograma in range(df_programas.shape[0]):
                
                nombre_programa = df_programas['nombre_programa'].iloc[iprograma]
        
                for ifecha in range(len(fechas_comparacion)):
                    
                    fecha_consulta = fechas_comparacion[ifecha]
                    df_estados     = FUNCIONES_AUXILIARES.comprueba_estado(nombre_programa,fecha_consulta,nombre_estados,df_estado_procesos)
                          
                    for ivalor in range(len(nombre_estados)):
                        try:
                            num_valores[ifecha,iprograma,ivalor] = df_estados.Estado.value_counts()[nombre_estados[ivalor]]
                        except:
                            pass
        
                    
        
        
            # Representa el gráfico
            fig, ax           = plt.subplots()
            anchura_barra     = 0.125
            altura_base       = 1.5
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
                etiqueta_altura                   = valores_acumulados[:,-1] + altura_base
                valor_maximo_programa [iprograma] = max(etiqueta_altura)
                
                # Representa la barra correspondiente a cada estado, en los distintos tiempos considerados
                for igrafico in range(num_valores.shape[2]):
                    
                    posicion_fondo = acumulados_mod[:,igrafico]
                    
                    plt.bar(posicion_x_programa, num_valores[:,iprograma,igrafico], anchura_barra, bottom = posicion_fondo ,color=colores_estados[igrafico],edgecolor='k')
            
                # Añade una etiqueta para identificar al programa
                etiqueta_nombre   = [etiquetas[iprograma]]*num_valores.shape[0]
                for ifecha in range(num_valores.shape[0]):
                    
                    # Etiqueta con el nombre del programa
                    angulo_giro = 90
                    if etiqueta_altura[ifecha] > altura_base:
                        ax.text(posicion_x_programa[ifecha], etiqueta_altura[ifecha], etiqueta_nombre[ifecha], ha="center", va="bottom",rotation=angulo_giro)
                    
                    # Etiqueta con el valor de cada uno de los estados
                    if valores_programa[ifecha,0] > 0:
                        ax.text(posicion_x_programa[ifecha], valores_acumulados[ifecha,0] , str(valores_programa[ifecha,0]), ha="center", va="bottom")
                    if valores_programa[ifecha,1] > 0:
                        ax.text(posicion_x_programa[ifecha], valores_acumulados[ifecha,1] , str(valores_programa[ifecha,1]), ha="center", va="bottom")
                    if valores_programa[ifecha,2] > 0:
                        ax.text(posicion_x_programa[ifecha], valores_acumulados[ifecha,2] , str(valores_programa[ifecha,2]), ha="center", va="bottom")
                    if valores_programa[ifecha,3] > 0:
                        ax.text(posicion_x_programa[ifecha], valores_acumulados[ifecha,3] , str(valores_programa[ifecha,3]), ha="center", va="bottom")
            
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
    entradas     = ['Añadir Run en proceso', 'Run terminado','Campaña terminada']
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
        
            nombre_muestras = st.text_input('Nombre del run', value="")
            
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
        
            col1, col2= st.columns(2,gap="small")
            with col1:
                fecha_inicio       = st.date_input('Fecha de inicio',value=fecha_actual)
            with col2:
                fecha_estimada_fin = st.date_input('Fecha estimada de finalizacion',min_value=fecha_actual,value=fecha_actual)

            observaciones_muestras = st.text_input('Observaciones', value="")
        
            # Botón de envío para confirmar selección
            submit = st.form_submit_button("Enviar")
    
            if submit == True:
                
                conn = init_connection()
                cursor = conn.cursor()           
                instruccion_sql = "INSERT INTO procesado_actual_nutrientes (nombre_proceso,programa,nombre_programa,año,num_muestras,fecha_inicio,fecha_estimada_fin,fecha_real_fin,observaciones,io_estado) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (nombre_proceso) DO UPDATE SET (programa,año,nombre_programa,num_muestras,fecha_inicio,fecha_estimada_fin,fecha_real_fin,observaciones,io_estado) = (EXCLUDED.programa,EXCLUDED.año,EXCLUDED.nombre_programa,EXCLUDED.num_muestras,EXCLUDED.fecha_inicio,EXCLUDED.fecha_estimada_fin,EXCLUDED.fecha_real_fin,EXCLUDED.observaciones,EXCLUDED.io_estado);"   
                cursor.execute(instruccion_sql, (nombre_muestras,id_programa_elegido,nombre_programa,anho_consulta,num_muestras,fecha_inicio,fecha_estimada_fin,None,observaciones_muestras,1)) 
                conn.commit() 
                cursor.close()
                conn.close()  
                
                texto_exito = 'Muestras ' + nombre_muestras + ' añadidas a la cola de procesado'
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


    if tipo_entrada == entradas[2]:

        st.header('Campaña/proyecto terminado')
        
        # Busca el año actual para limitar la fecha de entrada 
        anho_actual = fecha_actual.year
    
        # Recupera la tabla de los programas disponibles como un dataframe
        conn = init_connection()
        df_procesos = psql.read_sql('SELECT * FROM estado_procesos', conn)
        conn.close()
        
        # Listado tareas
        procesos = ['Análisis de muestras en laboratorio','Post-Procesado y control de calidad']
        
        # Despliega un formulario para seleccionar el proyecto o campaña terminado
        with st.form("Formulario seleccion"):
                   
           
            col1, col2= st.columns(2,gap="small")
            
            with col1:
                
                listado_programas        = df_procesos['nombre_programa'].unique()
                programa_seleccionado    = st.selectbox('Programa ',(listado_programas))
                
                df_programa_seleccionado = df_procesos[df_procesos['nombre_programa']==programa_seleccionado]
                df_programa_seleccionado = df_programa_seleccionado.sort_values('año')
                                   
            with col2:               
                anho_procesado            = st.selectbox('Año ',(df_programa_seleccionado['año']))
                id_proceso                = df_programa_seleccionado['id_proceso'][df_programa_seleccionado['año']==anho_procesado]
 
            tipo_proceso              = st.selectbox('Tipo de proceso terminado ',(procesos))
            id_tipo_proceso           = procesos.index(tipo_proceso)
        
            col1, col2= st.columns(2,gap="small")
            
            with col1:        
        
                fecha_actualiza  = st.date_input('Fecha de finalización ',max_value=fecha_actual,value=fecha_actual)

            with col2:        
        
                correo_contacto  = st.text_input('Correo de contacto del responsable ', value="")
        
                if id_tipo_proceso == 0:
                    campo_fecha    = 'fecha_analisis_laboratorio'
                    campo_contacto = 'contacto_analisis_laboratorio'
                else:
                    campo_fecha    = 'fecha_post_procesado'                    
                    campo_contacto = 'contacto_analisis_laboratorio'
                    
                submit = st.form_submit_button("Enviar")
    
                if submit == True:
                    
                    fecha_actual = datetime.date.today()
                    
                    conn = init_connection()
                    cursor = conn.cursor() 
                    instruccion_sql = "UPDATE estado_procesos SET " + campo_fecha + " = %s," + campo_contacto + " = %s WHERE id_proceso = %s;"
                    cursor.execute(instruccion_sql, (fecha_actualiza,correo_contacto,int(id_proceso)))                
                    conn.commit() 
                    cursor.close()
                    conn.close()  
                    
                    texto_exito = 'Estado del procesado de ' + programa_seleccionado + ' ' + str(anho_procesado) + ' actualizado correctamente'
                    st.success(texto_exito)
                    
                    st.experimental_rerun()


    
###############################################################################
#################### PÁGINA DE PROCESOS EN CURSO ##############################
###############################################################################    
    
def consulta_procesos():
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    entradas     = ['Análisis en curso', 'Análisis realizados en un periodo de tiempo']
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
                    st.dataframe(df_muestreos_terminados,height=150,use_container_width=True)
                else:
                    
                    texto_error = 'No hay ninguna muestra en proceso durante el periodo de tiempo consultado (' + fecha_inicio_consulta.strftime("%Y/%m/%d") + '-' + fecha_final_consulta.strftime("%Y/%m/%d") + ')'
                    st.warning(texto_error, icon="⚠️") 

                st.subheader('Listado de procesos terminados')

                if df_muestreos_terminados.shape[0] > 0:
                        
                    # Muestra una tabla con los análisis en curso
                    st.dataframe(df_muestreos_terminados,height=300,use_container_width=True)
                    # altura_tabla = 300
                    # gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(df_muestreos_terminados)
                    # gridOptions = gb.build()
                    # st_aggrid.AgGrid(df_muestreos_terminados,gridOptions=gridOptions,height = altura_tabla,enable_enterprise_modules=True,allow_unsafe_jscode=True)    
            
                else:
                    
                    texto_error = 'No se terminó ningún análisis durante el periodo de tiempo consultado (' + fecha_inicio_consulta.strftime("%Y/%m/%d") + '-' + fecha_final_consulta.strftime("%Y/%m/%d") + ')'
                    st.warning(texto_error, icon="⚠️")  
                
                    

        
 





###############################################################################
################# PÁGINA DE ENTRADA DE SALIDAS A MAR ##########################
###############################################################################    
    
def entrada_salidas_mar():
    
    # Función para cargar en caché los datos a utilizar
    @st.cache_data(ttl=300,show_spinner="Cargando información de la base de datos")
    def carga_datos_salidas_mar():
        conn                      = init_connection()
        df_buques            = psql.read_sql('SELECT * FROM buques', conn)
        df_config_perfilador = psql.read_sql('SELECT * FROM configuracion_perfilador', conn)
        df_config_superficie = psql.read_sql('SELECT * FROM configuracion_superficie', conn)
        df_personal          = psql.read_sql('SELECT * FROM personal_salidas', conn)
        df_salidas           = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_estaciones        = psql.read_sql('SELECT * FROM estaciones', conn)
        df_programas         = psql.read_sql('SELECT * FROM programas', conn)
        
        conn.close()
        return df_buques,df_config_perfilador,df_config_superficie,df_personal,df_salidas,df_estaciones,df_programas
    
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port
    
    # Cargar los datos de la caché
    df_buques,df_config_perfilador,df_config_superficie,df_personal,df_salidas,df_estaciones,df_programas = carga_datos_salidas_mar()
    
    # Procesa cierta información utilizada en todos los procesos
    id_radiales                = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]

    df_salidas_radiales        = df_salidas[df_salidas['programa']==int(id_radiales)]
    df_estaciones_radiales     = df_estaciones[df_estaciones['programa']==int(id_radiales)]
  
    # Fechas y horas por defecto
    fecha_actual        = datetime.date.today()
    hora_defecto_inicio = datetime.time(8,30,0,0,tzinfo = datetime.timezone.utc)
    hora_defecto_final  = datetime.time(14,30,0,0,tzinfo = datetime.timezone.utc)  
  
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    entradas     = ['Añadir salida al mar','Modificar salidas realizadas','Consultar salidas realizadas','Personal participante']
    tipo_entrada = st.sidebar.radio("Indicar la consulta a realizar",entradas)
    

    # Añade una salida al mar
    if tipo_entrada == entradas[0]:  
        
        st.subheader('Salida al mar')

        # tipos de salida en las radiales
        tipos_radiales = ['MENSUAL','SEMANAL']

        # Despliega los formularios 
        with st.form("Formulario seleccion"):
                   
            col1, col2 = st.columns(2,gap="small")
            
            with col1:
                tipo_salida     = st.selectbox('Tipo de radial',(tipos_radiales))
           
            with col2:
                nombre_salida        = st.text_input('Nombre de la salida', value="")                
            

            # Selección de fechas, personal, y estaciones        
            personal_comisionado_previo    = None
            personal_no_comisionado_previo = None
            estaciones_previas             = None 
            id_buque_previo                = None 
            id_perfil_previo               = None 
            id_sup_previo                  = None
            fecha_salida,hora_salida,fecha_regreso,hora_regreso,json_comisionados,json_no_comisionados,json_estaciones,id_buque_elegido,id_configurador_perfil,id_configurador_sup = FUNCIONES_AUXILIARES.menu_metadatos_radiales(fecha_actual,hora_defecto_inicio,fecha_actual,hora_defecto_final,df_personal,personal_comisionado_previo,personal_no_comisionado_previo,estaciones_previas,df_estaciones_radiales,id_buque_previo,df_buques,id_perfil_previo,df_config_perfilador,id_sup_previo,df_config_superficie)
    
                    
            # Selección de variables medidas con botellas y continuo
            json_variables_previas = []
            json_variables    = FUNCIONES_AUXILIARES.menu_variables_radiales(json_variables_previas)
            json_variables    = json.dumps(json_variables)
                    
            json_variables         = json.dumps(json_variables)

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
                        if df_salidas_radiales['fecha_salida'].iloc[isalida] == fecha_salida and df_salidas_radiales['tipo_salida'].iloc[isalida] == tipo_salida:
                            io_incluido = 1
    
                    if io_incluido == 0:   
                        
                        # Determina el id de la salida
                        id_salida = int(max(df_salidas['id_salida']) + 1)
                        
                        instruccion_sql = '''INSERT INTO salidas_muestreos (id_salida,nombre_salida,programa,nombre_programa,tipo_salida,fecha_salida,hora_salida,fecha_retorno,hora_retorno,buque,participantes_comisionados,participantes_no_comisionados,observaciones,estaciones,variables_muestreadas,configuracion_perfilador,configuracion_superficie)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id_salida) DO NOTHING;''' 
                                
                        conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                        cursor = conn.cursor()
                        cursor.execute(instruccion_sql, (id_salida,nombre_salida,3,'RADIAL CORUÑA',tipo_salida,fecha_salida,hora_salida,fecha_regreso,hora_regreso,id_buque_elegido,json_comisionados,json_no_comisionados,observaciones,json_estaciones,json_variables,id_configurador_perfil,id_configurador_sup))
                        conn.commit()
                        cursor.close()
                        conn.close()
        
                        texto_exito = 'Salida añadida correctamente'
                        st.success(texto_exito)
                        
                        st.cache_data.clear()
                        
                    else:
                        texto_error = 'La base de datos ya contiene una salida ' + tipo_salida.lower() + ' para la fecha seleccionada. Puede modificar los datos de esa salida en la pestaña correspondiente'
                        st.warning(texto_error, icon="⚠️")                      
                        
    
    
    # Modificar datos correspondientes a una salida de radial
    if tipo_entrada == entradas[1]:                    

        # Modifica una salida
        st.subheader('Modifica salida al mar')
                         
        
        # Despliega menús de selección del programa, tipo de salida, año y fecha               
        col1, col2= st.columns(2,gap="small")
     
        with col1:
            tipo_salida_seleccionada  = st.selectbox('Tipo de salida',(df_salidas_radiales['tipo_salida'].unique()))   
            df_salidas_seleccion      = df_salidas_radiales[df_salidas_radiales['tipo_salida']==tipo_salida_seleccionada]
        
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
        
        with col2:
            anho_seleccionado           = st.selectbox('Año',(listado_anhos),index=len(listado_anhos)-1)
            df_salidas_seleccion        = df_salidas_seleccion[df_salidas_seleccion['año']==anho_seleccionado]
    
        salida                      = st.selectbox('Muestreo',(df_salidas_seleccion['nombre_salida']),index=df_salidas_seleccion.shape[0]-1)   
    
        # Despliega un formulario para modificar los datos de la salida 
        with st.form("Formulario seleccion"):    
    
                    
            datos_salida_seleccionada      = df_salidas[df_salidas['nombre_salida']==salida]
            id_salida                      = datos_salida_seleccionada['id_salida'].iloc[0]
            
            # Selección de fechas, personal, y estaciones
            fecha_salida                   = datos_salida_seleccionada['fecha_salida'].iloc[0]
            hora_salida                    = datos_salida_seleccionada['hora_salida'].iloc[0]
            fecha_regreso                  = datos_salida_seleccionada['fecha_retorno'].iloc[0]
            hora_regreso                   = datos_salida_seleccionada['hora_retorno'].iloc[0]
            personal_comisionado_previo    = datos_salida_seleccionada['participantes_comisionados'].iloc[0]
            personal_no_comisionado_previo = datos_salida_seleccionada['participantes_no_comisionados'].iloc[0]
            estaciones_previas             = datos_salida_seleccionada['estaciones'].iloc[0] 
            id_buque_previo                = datos_salida_seleccionada['buque'].iloc[0] - 1
            
            
            
            
            listado_config_perfilador      = datos_salida_seleccionada['configuracion_perfilador']
            listado_config_perfilador      = [ int(x) for x in listado_config_perfilador]
            listado_config_sup             = datos_salida_seleccionada['configuracion_superficie']
            listado_config_sup             = [ int(x) for x in listado_config_sup]            
            id_perfil_previo               = listado_config_perfilador.index(int(datos_salida_seleccionada['configuracion_perfilador'].iloc[0]))
            id_sup_previo                  = listado_config_sup.index(int(datos_salida_seleccionada['configuracion_superficie'].iloc[0]))
            
            fecha_salida,hora_salida,fecha_regreso,hora_regreso,json_comisionados,json_no_comisionados,json_estaciones,id_buque_elegido,id_configurador_perfil,id_configurador_sup = FUNCIONES_AUXILIARES.menu_metadatos_radiales(fecha_salida,hora_salida,fecha_regreso,hora_regreso,df_personal,personal_comisionado_previo,personal_no_comisionado_previo,estaciones_previas,df_estaciones_radiales,id_buque_previo,df_buques,id_perfil_previo,df_config_perfilador,id_sup_previo,df_config_superficie)
            
            # Selección de fechas, personal, y estaciones
            json_variables_previas         = datos_salida_seleccionada['variables_muestreadas'].iloc[0]
            
            json_variables                 = FUNCIONES_AUXILIARES.menu_variables_radiales(json_variables_previas)
            json_variables                 = json.dumps(json_variables)

            observaciones_previas          = datos_salida_seleccionada['observaciones'].iloc[0] 
            observaciones                  = st.text_input('Observaciones', value=observaciones_previas)
    
            submit = st.form_submit_button("Actualizar salida")
    
            if submit == True:
                           
                instruccion_sql = '''UPDATE salidas_muestreos SET fecha_salida = %s, hora_salida = %s, fecha_retorno = %s,hora_retorno = %s, buque = %s, participantes_comisionados = %s ,participantes_no_comisionados = %s,observaciones = %s,estaciones = %s,variables_muestreadas = %s WHERE id_salida = %s;''' 
                        
                conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()
                cursor.execute(instruccion_sql, (fecha_salida,hora_salida,fecha_regreso,hora_regreso,int(id_buque_elegido),json_comisionados,json_no_comisionados,observaciones,json_estaciones,json_variables,int(id_salida)))
                conn.commit()
                cursor.close()
                conn.close()


                texto_exito = 'Salida ' + salida + ' actualizada correctamente'
                st.success(texto_exito)
                
                st.cache_data.clear()



    # Consulta las salidas realizadas
    if tipo_entrada == entradas[2]: 
        
        st.subheader('Consultar salidas al mar realizadas')
       
        # Añade una columna con el nombre del buque utilizado
        df_salidas_radiales['Buque'] = None
        for isalida in range(df_salidas_radiales.shape[0]):
            if df_salidas_radiales['buque'].iloc[isalida].is_integer():
                df_salidas_radiales['Buque'].iloc[isalida] = df_buques['nombre_buque'][df_buques['id_buque']==df_salidas_radiales['buque'].iloc[isalida]].iloc[0]

        # Elimina las columnas que no interesa mostrar
        df_salidas_radiales = df_salidas_radiales.drop(columns=['id_salida','programa','nombre_programa','buque','configuracion_perfilador','configuracion_superficie'])
    
        # Renombra las columnas
        df_salidas_radiales = df_salidas_radiales.rename(columns={'nombre_salida':'Salida','tipo_salida':'Tipo','fecha_salida':'Fecha salida','hora_salida':'Hora salida','fecha_retorno':'Fecha retorno','hora_retorno':'Hora retorno','observaciones':'Observaciones','estaciones':'Estaciones muestreadas','participantes_comisionados':'Participantes comisionados','participantes_no_comisionados':'Participantes no comisionados'})
    
        # Ajusta el formato de las fechas
        for idato in range(df_salidas_radiales.shape[0]):
            df_salidas_radiales['Fecha salida'].iloc[idato]   =  df_salidas_radiales['Fecha salida'].iloc[idato].strftime("%Y-%m-%d")
            df_salidas_radiales['Fecha retorno'].iloc[idato]  =  df_salidas_radiales['Fecha retorno'].iloc[idato].strftime("%Y-%m-%d")

        # Ordena los valores por fechas
        df_salidas_radiales = df_salidas_radiales.sort_values('Fecha salida',ascending=False)

        # Mueve los identificadores de muestreo al final del dataframe
        listado_cols = df_salidas_radiales.columns.tolist()
        listado_cols.append(listado_cols.pop(listado_cols.index('Observaciones')))   
        df_salidas_radiales = df_salidas_radiales[listado_cols]
        
        #st.text(df_salidas_radiales['variables_muestreadas'])
          
        # Muestra una tabla con las salidas realizadas
        st.dataframe(df_salidas_radiales,use_container_width=True)

        # Botón para descargar las salidas disponibles
        nombre_archivo =  'DATOS_SALIDAS.xlsx'
    
        output = BytesIO()
        writer = pandas.ExcelWriter(output, engine='xlsxwriter')
        df_salidas_radiales.to_excel(writer, index=False, sheet_name='DATOS')
        # workbook = writer.book
        # worksheet = writer.sheets['DATOS']
        writer.sheets['DATOS']        
        writer.save()
        df_salidas_radiales = output.getvalue()
    
        st.download_button(
            label="DESCARGA EXCEL CON LAS SALIDAS REALIZADAS",
            data=df_salidas_radiales,
            file_name=nombre_archivo,
            help= 'Descarga un archivo .csv con los datos solicitados',
            mime="application/vnd.ms-excel")
        



    # Añade personal participante en las salidas de radial
    if tipo_entrada == entradas[3]: 

        st.subheader('Personal participante')
        
        # Muestra una tabla con el personal ya incluido en la base de datos
        st.dataframe(df_personal,height=250)
                
        st.subheader('Añadir personal participante')
        # Despliega un formulario para introducir los datos
        with st.form("Formulario seleccion"):
                   
            nombre_participante  = st.text_input('Nombre y apellidos del nuevo personal', value="")
            
            comision             = st.checkbox('Comisionado')
            
            submit = st.form_submit_button("Añadir participante")

            if submit == True:

                io_incluido = 0
                for ipersonal in range(df_personal.shape[0]):
                    if df_personal['nombre_apellidos'][ipersonal] == nombre_participante:
                        io_incluido = 1
                
                if io_incluido == 0:

                    id_asignado =  int(max(df_personal['id_personal']) + 1)

                    instruccion_sql = '''INSERT INTO personal_salidas (id_personal,nombre_apellidos,comisionado)
                        VALUES (%s,%s,%s) ON CONFLICT (id_personal) DO UPDATE SET (nombre_apellidos,comisionado) = ROW(EXCLUDED.nombre_apellidos,EXCLUDED.comisionado);''' 
                                            
                    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()
                    cursor.execute(instruccion_sql, (id_asignado,nombre_participante,comision))                     
                    conn.commit()
                    cursor.close()
                    conn.close()

                    texto_exito = 'Participante añadido correctamente'
                    st.success(texto_exito)
                    
                    st.cache_data.clear()
        
                else:
                    texto_error = 'El participante introducido ya se encuentra en la base de datos '
                    st.warning(texto_error, icon="⚠️")  



        
               









###############################################################################
################# PÁGINA DE ENTRADA DE DATOS DEL ESTADO DEL  MAR ##############
###############################################################################    

def entrada_condiciones_ambientales():
    
    # Función para cargar en caché los datos a utilizar
    @st.cache_data(ttl=600,show_spinner="Cargando información de la base de datos")
    def carga_datos_condiciones_ambientales():
        conn                      = init_connection()
        df_salidas      = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas    = psql.read_sql('SELECT * FROM programas', conn)
        df_estaciones   = psql.read_sql('SELECT * FROM estaciones', conn)
        df_condiciones  = psql.read_sql('SELECT * FROM condiciones_ambientales_muestreos', conn)
        conn.close()
        return df_salidas,df_programas,df_estaciones,df_condiciones
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host = st.secrets["postgres"].host
    base_datos     = st.secrets["postgres"].dbname
    usuario        = st.secrets["postgres"].user
    contrasena     = st.secrets["postgres"].password
    puerto         = st.secrets["postgres"].port
    
    # Carga la informacion de la cache 
    df_salidas,df_programas,df_estaciones,df_condiciones = carga_datos_condiciones_ambientales()
    
    id_radiales            = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].iloc[0]

   
    df_salidas_radiales    = df_salidas[df_salidas['programa']==id_radiales] 
    df_estaciones_radiales = df_estaciones[df_estaciones['programa']==id_radiales]
    
    # Ajusta el formato de las fechas
    for idato in range(df_salidas_radiales.shape[0]):
        df_salidas_radiales['fecha_salida'].iloc[idato]  =  df_salidas_radiales['fecha_salida'].iloc[idato].strftime("%Y-%m-%d")
   
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    entradas     = ['Añade o modifica condiciones ambientales', 'Descarga datos de condiciones ambientales']
    tipo_entrada = st.sidebar.radio("Indicar la consulta a realizar",entradas)
    

    # Añade o modifica datos 
    if tipo_entrada == entradas[0]:
        
        st.subheader('Entrada de datos de condiciones ambientales')
        
        # Vectores con los valores de las variables que tienen opciones concretas
        seleccion_SN = ['Si','No']
        direcciones  = [None,'N','NW','W','SW','S','SE','E','NE'] 
        
        estado_mar_nombre = ['Calma (0)','Mar rizada (1)','Marejadilla (2)', 'Marejada (3)', 'Fuerte marejada (4)', 'Gruesa (5)', 'Muy Gruesa (6)']
        estado_mar_hmin   = [0, 0  , 0.1 , 0.50 , 1.25, 2.5, 4]
        estado_mar_hmax   = [0, 0.1, 0.5 , 1.25 , 2.50, 4.0, 6]
    
        mareas          = ['Baja','Media','Pleamar']
        
        # Selecciona la salida de la que se quiere introducir datos
        df_salidas_radiales    = df_salidas_radiales.sort_values('fecha_salida',ascending=False)
        salida                 = st.selectbox('Salida',(df_salidas_radiales['nombre_salida']))   
        id_salida              = df_salidas_radiales['id_salida'][df_salidas_radiales['nombre_salida']==salida].iloc[0]
    
        # Aviso de que ya hay información de esa salida y muestra la información    
        df_condiciones_salida_seleccionada = df_condiciones[(df_condiciones['salida']==id_salida)]
        if df_condiciones_salida_seleccionada.shape[0] > 0:
            texto_error = 'Ya existen datos correspondientes a la salida seleccionada.'
            st.warning(texto_error, icon="⚠️")       
    
            for idato in range(df_condiciones_salida_seleccionada.shape[0]):
                df_condiciones_salida_seleccionada['estacion'].iloc[idato] =  df_estaciones['nombre_estacion'][df_estaciones['id_estacion'] == df_condiciones_salida_seleccionada['estacion'].iloc[idato]] 

        # Extrae las estaciones visitadas en la salida seleccionada
        listado_estaciones = df_salidas_radiales['estaciones'][df_salidas_radiales['id_salida']==id_salida].iloc[0] 
    
        # Selecciona la estación de la que se quiere introducir datos (entre todas las disponibles)
        estacion_elegida    = st.selectbox('Estacion',(listado_estaciones))
        id_estacion_elegida = int(df_estaciones_radiales['id_estacion'][df_estaciones_radiales['nombre_estacion']==estacion_elegida].values[0])
    
        # recupera los datos disponibles en la base de datos para asignar valores por defecto
        df_condicion_introducida = df_condiciones[(df_condiciones['salida']==id_salida) & (df_condiciones['estacion']==id_estacion_elegida)]               
            
        if df_condicion_introducida.shape[0] == 1:
            
               
            # Asigna como valores por defecto los que ya estaban en la base de datos
            hora_llegada_defecto            = df_condicion_introducida['hora_llegada'].iloc[0]
            profundidad_defecto             = df_condicion_introducida['profundidad'].iloc[0]
            nubosidad_defecto               = df_condicion_introducida['nubosidad'].iloc[0]
            
            if df_condicion_introducida['lluvia'].iloc[0] is not None:
                indice_lluvia_defecto           = seleccion_SN.index(df_condicion_introducida['lluvia'].iloc[0])
            else:
                indice_lluvia_defecto       = 0
            velocidad_viento_defecto        = df_condicion_introducida['velocidad_viento'].iloc[0]
            if df_condicion_introducida['direccion_viento'].iloc[0] is not None:
                indice_direccion_viento_defecto = direcciones.index(df_condicion_introducida['direccion_viento'].iloc[0])
            else:
                indice_direccion_viento_defecto = 0
            pres_atmosferica_defecto        = df_condicion_introducida['pres_atmosferica'].iloc[0]
            altura_ola_defecto              = df_condicion_introducida['altura_ola'].iloc[0]
            if df_condicion_introducida['mar_fondo'].iloc[0] is not None:
                indice_mar_fondo_defecto        = seleccion_SN.index(df_condicion_introducida['mar_fondo'].iloc[0])
            else:
                indice_mar_fondo_defecto    = 0
            if df_condicion_introducida['mar_direccion'].iloc[0] is not None:
                indice_mar_direccion_defecto    = direcciones.index(df_condicion_introducida['mar_direccion'].iloc[0])
            else:
                indice_mar_direccion_defecto = 0
            temp_aire_defecto               = df_condicion_introducida['temp_aire'].iloc[0]
            
            if df_condicion_introducida['marea'].iloc[0] is not None:
                indice_marea_defecto        = mareas.index(df_condicion_introducida['marea'].iloc[0])
            else:
                indice_marea_defecto        = 0
                
            if df_condicion_introducida['prof_secchi'].iloc[0] is not None:
                prof_secchi_defecto         = df_condicion_introducida['prof_secchi'].iloc[0]
            else:
                prof_secchi_defecto         = 0
                
            if df_condicion_introducida['max_clorofila'].iloc[0] is not None:
                max_clorofila_defecto       = df_condicion_introducida['max_clorofila'].iloc[0]
            else:
                max_clorofila_defecto       = 0

            if df_condicion_introducida['humedad_relativa'].iloc[0] is not None:                
                humedad_relativa_defecto    = df_condicion_introducida['humedad_relativa'].iloc[0]
            else:
                humedad_relativa_defecto    = 50
            
            if df_condicion_introducida['temp_superficie'].iloc[0] is not None:
                temp_superficie_defecto     = df_condicion_introducida['temp_superficie'].iloc[0]
            else:
                temp_superficie_defecto     = 16                
            
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
            temp_superficie_defecto         = 16
            
            io_previo                       = 0
                       
         
        with st.form("Formulario seleccion"): 
               
            texto_estacion  = 'Estacion ' + estacion_elegida
            st.write(texto_estacion)
            
            col1, col2,col3,col4= st.columns(4,gap="small")
    
    
            with col1:
                hora_llegada  = st.time_input('Hora de llegada (local)',value=hora_llegada_defecto,step=int(300))
                profundidad   = st.number_input('Profundidad (m):',format='%i',value=int(profundidad_defecto),min_value=0)
                nubosidad        = st.number_input('Nubosidad (%) :',format='%i',value=int(nubosidad_defecto),min_value=0)
                lluvia           = st.selectbox('LLuvia:',(seleccion_SN),index=indice_lluvia_defecto)
                                       
            with col2:
                velocidad_viento  = st.number_input('Vel.Viento (m/s):',value=float(velocidad_viento_defecto),min_value=float(0),step =0.5)
                direccion_viento  = st.selectbox('Dir.Viento:',(direcciones),index = indice_direccion_viento_defecto)
                pres_atmosferica  = st.number_input('Presion atm. (mmHg):',format='%i',value=int(pres_atmosferica_defecto),min_value=0)
                humedad_relativa = st.number_input('Humedad relativa (%):',value=int(humedad_relativa_defecto),min_value=0)
                
    
            with col3:
                 altura_ola  = st.number_input('Altura de ola (m):',value=float(altura_ola_defecto),min_value=float(0),step =0.5)
                 indice_prop = len(estado_mar_nombre)-1
                 for idato_estado in range(len(estado_mar_nombre)):    
                     if altura_ola >= estado_mar_hmin[idato_estado] and altura_ola <= estado_mar_hmax[idato_estado]:
                         indice_prop = idato_estado
                 #mar_douglas = st.selectbox('Mar Douglas:',(douglas_nombre),index=indice_prop)
                 estado_mar    = st.selectbox('Estado del mar:',(estado_mar_nombre),index = indice_prop)
                 mar_fondo     = st.selectbox('Mar de fondo:',(seleccion_SN),index = indice_mar_fondo_defecto)
                 mar_direccion = st.selectbox('Dir.Oleaje:',(direcciones),index = indice_mar_direccion_defecto)
                 
    
            with col4:
                 temp_aire        = st.number_input('Temperatura del aire(ºC):',value=float(temp_aire_defecto),min_value=float(0),step=0.1)
                 temp_superf      = st.number_input('Temperatura superficie (ºC):',value=float(temp_superficie_defecto),min_value=float(0),step=0.1)    
                 marea            = st.selectbox('Marea:',(mareas),index = indice_marea_defecto)
                 prof_secchi      = st.number_input('Prof.Sechi(m):',value=float(prof_secchi_defecto),min_value=float(0),step=0.5)           
                 max_clorofila    = st.number_input('Max.Clorofila(m):',value=float(max_clorofila_defecto),min_value=float(0),step=0.5)


    
            submit = st.form_submit_button("Añadir o modificar datos")                    
    
            if submit is True:
                
                conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()
                
                if io_previo == 1:
 
                    instruccion_sql_consulta = '''SELECT id_condicion FROM condiciones_ambientales_muestreos WHERE salida = %s and estacion = %s ''' 
                    cursor.execute(instruccion_sql_consulta, (int(id_salida),int(id_estacion_elegida)))
                    id_condicion = cursor.fetchone()[0]
                    
                else:
                    
                    id_condicion = max(df_condiciones['id_condicion']) + 1  
                
                instruccion_sql = '''INSERT INTO condiciones_ambientales_muestreos (id_condicion,salida,estacion,hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,mar_direccion,humedad_relativa,temp_aire,marea,prof_secchi,max_clorofila,estado_mar,temp_superficie)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (salida,estacion) DO UPDATE SET (id_condicion,hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,mar_direccion,humedad_relativa,temp_aire,marea,prof_secchi,max_clorofila,estado_mar,temp_superficie) = ROW(EXCLUDED.id_condicion,EXCLUDED.hora_llegada,EXCLUDED.profundidad,EXCLUDED.nubosidad,EXCLUDED.lluvia,EXCLUDED.velocidad_viento,EXCLUDED.direccion_viento,EXCLUDED.pres_atmosferica,EXCLUDED.altura_ola,EXCLUDED.mar_fondo,EXCLUDED.mar_direccion,EXCLUDED.humedad_relativa,EXCLUDED.temp_aire,EXCLUDED.marea,EXCLUDED.prof_secchi,EXCLUDED.max_clorofila,EXCLUDED.estado_mar,EXCLUDED.temp_superficie);''' 
                        
                cursor.execute(instruccion_sql, (int(id_condicion),int(id_salida),int(id_estacion_elegida),hora_llegada,profundidad,nubosidad,lluvia,velocidad_viento,direccion_viento,pres_atmosferica,altura_ola,mar_fondo,mar_direccion,humedad_relativa,temp_aire,marea,prof_secchi,max_clorofila,estado_mar,temp_superf))
                conn.commit()
                cursor.close()
                conn.close()
    
                if io_previo == 0:
                    texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' añadidos correctamente'
                if io_previo == 1:
                    texto_exito = 'Datos de las estación ' + estacion_elegida + ' durante la salida '  + salida  + ' actualizados correctamente'
                    
                st.success(texto_exito)                
                 
        st.cache_data.clear()
                
    # Descarga datos ambientales
    if tipo_entrada == entradas[1]:    
        
        st.subheader('Descarga de datos de condiciones ambientales')

        # Selecciona las salidas de la que se quieren descargar los datos
        with st.form("Formulario seleccion"): 
            
            df_salidas_radiales    = df_salidas_radiales.sort_values('fecha_salida',ascending=False)
            
            listado_salidas        = st.multiselect('Muestreo',(df_salidas_radiales['nombre_salida'])) 

            submit = st.form_submit_button("Confirmar selección")                    
    
        if submit is True:
    
            identificadores_salidas         = numpy.zeros(len(listado_salidas),dtype=int)
            for idato in range(len(listado_salidas)):
                identificadores_salidas[idato] = df_salidas_radiales['id_salida'][df_salidas_radiales['nombre_salida']==listado_salidas[idato]].iloc[0]
        
            df_salidas_seleccion = df_condiciones[df_condiciones['salida'].isin(identificadores_salidas)]       
    
            # Asigna nombres de salida, estaciones y fecha, y elimina el identificador de los datos ambientales
            df_salidas_seleccion['fecha'] = None
            for idato in range(df_salidas_seleccion.shape[0]):
                df_salidas_seleccion['fecha'].iloc[idato]   = df_salidas_radiales['fecha_salida'][df_salidas_radiales['id_salida']==df_salidas_seleccion['salida'].iloc[idato]].iloc[0]
                df_salidas_seleccion['salida'].iloc[idato]   = df_salidas['nombre_salida'][df_salidas['id_salida']==df_salidas_seleccion['salida'].iloc[idato]].iloc[0]
                df_salidas_seleccion['estacion'].iloc[idato] = df_estaciones['nombre_estacion'][df_estaciones['id_estacion']==df_salidas_seleccion['estacion'].iloc[idato]].iloc[0]
                
            # mueve la columna con las fechas a la primera posicion
            df_salidas_seleccion = df_salidas_seleccion[ ['fecha'] + [ col for col in df_salidas_seleccion.columns if col != 'fecha' ] ]
                        
            # Botón para descargar las salidas disponibles
            nombre_archivo =  'DATOS_AMBIENTALES.xlsx'
        
            output = BytesIO()
            writer = pandas.ExcelWriter(output, engine='xlsxwriter')
            df_salidas_seleccion.to_excel(writer, index=False, sheet_name='DATOS')
            writer.sheets['DATOS']
            writer.save()
            df_salidas_seleccion = output.getvalue()
        
            st.download_button(
                label="DESCARGA EXCEL CON LOS DATOS DE LAS SALIDAS SELECCIONADAS",
                data=df_salidas_seleccion,
                file_name=nombre_archivo,
                help= 'Descarga un archivo .csv con los datos solicitados',
                mime="application/vnd.ms-excel"
            )
                




###############################################################################
##################### PÁGINA DE ENTRADA DE DATOS DE BOTELLAS ##################
###############################################################################    

def entrada_archivos_roseta():
    
    # Función para cargar en caché los datos a utilizar
    @st.cache_data(ttl=600,show_spinner='Recuperando información de la base de datos')
    def carga_datos_entrada_archivo_roseta():
        conn                      = init_connection()
        df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
        df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
        df_datos_discretos        = psql.read_sql('SELECT * FROM datos_discretos', conn)
        df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas              = psql.read_sql('SELECT * FROM programas', conn)
        df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
        conn.close()
        return df_muestreos,df_estaciones,df_datos_discretos,df_salidas,df_programas,df_indices_calidad
        
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port
    
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['Añadir datos de botellas y perfiles', 'Realizar control de calidad de datos de botellas']
    tipo_accion  = st.sidebar.radio("Indicar la acción a realizar",acciones)
    


    # Añade datos de botellas
    if tipo_accion == acciones[0]: 
        
        st.subheader('Entrada de datos procedentes de botellas y perfiles') 
    
        # Recupera tablas con informacion utilizada en el procesado
        df_muestreos,df_estaciones,df_datos_discretos,df_salidas,df_programas,df_indices_calidad = carga_datos_entrada_archivo_roseta()
        
        id_radiales   = df_programas.index[df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]

        with st.form("Formulario seleccion"): 
        
            # Despliega menús de selección del programa, tipo de salida, año y fecha               
            col1, col2, col3= st.columns(3,gap="small")
         
            with col1: 
                programa_seleccionado     = st.selectbox('Programa',(df_programas['nombre_programa']),index=id_radiales)   
                df_salidas_seleccion      = df_salidas[df_salidas['nombre_programa']==programa_seleccionado]
                abreviatura_programa      = df_programas['abreviatura'][df_programas['nombre_programa']==programa_seleccionado].iloc[0] 
                id_programa               = df_programas['id_programa'][df_programas['nombre_programa']==programa_seleccionado].iloc[0] 
            
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
           
            tabla_estaciones_programa = df_estaciones[df_estaciones['programa']==int(id_programa)]
    
        
            # Despliega la extensión para subir los archivos .btl y .cnv
            col1, col2 = st.columns(2,gap="small")
            
            with col1:
                listado_archivos_btl = st.file_uploader("Arrastra o selecciona los archivos .btl", accept_multiple_files=True)   
            with col2:
                listado_archivos_cnv = st.file_uploader("Arrastra o selecciona los archivos .cnv", accept_multiple_files=True)   
                 
            submit = st.form_submit_button("Procesar los archivos añadidos")                    
    
        if submit is True:
                                             
            for archivo_btl in listado_archivos_btl:
                            
                # encuentra el nombre de la estación
                nombre_archivo_btl = archivo_btl.name
                posicion_inicio    = nombre_archivo_btl.find('e') + 1
                posicion_final     = nombre_archivo_btl.find('.')
                nombre_estacion    = nombre_archivo_btl[posicion_inicio:posicion_final].upper() #+ 'CO' 
                id_estacion        = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
               

                texto_estado = 'Procesando la información de la estación ' + nombre_estacion
                with st.spinner(texto_estado):
                                    
                    # Lee los datos de cada archivo de botella
                    #datos_archivo = archivo_btl.getvalue().decode('utf-8').splitlines()
                    datos_archivo = archivo_btl.getvalue().decode('ISO-8859-1').splitlines()
                    
                    # Comprueba que la fecha del archivo y de la salida coinciden
                    fecha_salida_texto    = nombre_archivo_btl[0:8]
                    fecha_salida_archivo  = datetime.datetime.strptime(fecha_salida_texto, '%Y%m%d').date()
                    
                    if fecha_salida_archivo == fecha_salida:
                    
                        ### DATOS DE BOTELLERO ###
                        mensaje_error,datos_botellas,io_par,io_fluor,io_O2 = FUNCIONES_LECTURA.lectura_btl(nombre_archivo_btl,datos_archivo)
                      
                        datos_botellas['id_estacion_temp']         = id_estacion
                        datos_botellas['estacion']                 = id_estacion
                        datos_botellas['programa']                 = id_programa

                        profundidades_referencia = tabla_estaciones_programa['profundidades_referencia'][tabla_estaciones_programa['nombre_estacion']==nombre_estacion].iloc[0]
                        # Añade una columna con la profundidad de referencia
                        if profundidades_referencia is not None:
                            datos_botellas['prof_referencia'] = numpy.zeros(datos_botellas.shape[0],dtype=int)
                            for idato in range(datos_botellas.shape[0]):
                                    # Encuentra la profundidad de referencia más cercana a cada dato
                                    idx = (numpy.abs(profundidades_referencia - datos_botellas['presion_ctd'][idato])).argmin()
                                    datos_botellas['prof_referencia'][idato] =  profundidades_referencia[idx]
                        else:
                            datos_botellas['prof_referencia'] = [None]*datos_botellas.shape[0]

                        
                      
                        # Cambia los nombre de las botellas.        
                        if id_estacion == 1: #E2
                            listado_equiv_ctd = [1,3,5,7,9,11]
                            listado_equiv_real = [1,2,3,4,5,6]
                            for ibotella in range(datos_botellas.shape[0]):
                                for iequiv in range(len(listado_equiv_ctd)):
                                    if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                                        datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]
                                        
                            if id_estacion == 2: #E3A
                                listado_equiv_ctd = [1,3,5,7,9,11]
                                listado_equiv_real = [13,131,132,133,134,135]
                                for ibotella in range(datos_botellas.shape[0]):
                                    for iequiv in range(len(listado_equiv_ctd)):
                                        if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                                            datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]

                            if id_estacion == 3: #E3C
                                listado_equiv_ctd = [1,3,5,7,9,11]
                                listado_equiv_real = [14,141,142,143,144,145]
                                for ibotella in range(datos_botellas.shape[0]):
                                    for iequiv in range(len(listado_equiv_ctd)):
                                        if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                                            datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]
                                            
                            if id_estacion == 4: #E3B
                                listado_equiv_ctd = [1,3,5,7,9,11]
                                listado_equiv_real = [15,151,152,153,154,155]
                                for ibotella in range(datos_botellas.shape[0]):
                                    for iequiv in range(len(listado_equiv_ctd)):
                                        if datos_botellas['botella'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                                            datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]                                            
                                        

                        if id_estacion == 5: #E4
                            
                            datos_botellas['botella_temp'] = datos_botellas['botella']
                            datos_botellas = datos_botellas.drop(datos_botellas[datos_botellas.botella_temp == 11].index)
                            
                            listado_equiv_ctd = [1,3,5,7,9,11]
                            listado_equiv_real = [8,9,10,11,12,None]
                            for ibotella in range(datos_botellas.shape[0]):
                                for iequiv in range(len(listado_equiv_ctd)):
                                    if datos_botellas['botella_temp'].iloc[ibotella] == listado_equiv_ctd[iequiv]:
                                        datos_botellas['botella'].iloc[ibotella] = listado_equiv_real[iequiv]
                                        
                            datos_botellas = datos_botellas.drop(columns=['botella_temp'])  
                      
                        # Asigna lat/lon de la estación si esa información no etá incluia en el .btl
                        for imuestreo in range(datos_botellas.shape[0]):
                            if datos_botellas['latitud'].iloc[imuestreo] is None:
                                datos_botellas['latitud'].iloc[imuestreo] = tabla_estaciones_programa['latitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
                            if datos_botellas['longitud'].iloc[imuestreo] is None:
                                datos_botellas['longitud'].iloc[imuestreo] = tabla_estaciones_programa['longitud_estacion'][tabla_estaciones_programa['id_estacion']==id_estacion].iloc[0]
                                
                        # Asigna identificadores de salida al mar y estación
                        datos_botellas['id_estacion_temp'] = datos_botellas['estacion']
                        datos_botellas ['id_salida']       =  id_salida
                        
                        # Aplica control de calidad
                        datos_botellas,textos_aviso        = FUNCIONES_PROCESADO.control_calidad(datos_botellas)            
           
                        # Asigna el registro correspondiente a cada muestreo e introduce la información en la base de datos
                        datos_botellas = FUNCIONES_PROCESADO.evalua_registros(datos_botellas,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
             
                        FUNCIONES_PROCESADO.inserta_datos(datos_botellas,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)

                        
                    else:
                    
                        texto_error = 'La fecha del archivo ' + archivo_btl.name + ' no coindice con la fecha seleccionada '
                        st.warning(texto_error, icon="⚠️")  
                        
                        
                        
                    ### DATOS DE PERFIL
                    
                    for archivo_cnv in listado_archivos_cnv:
                    
                        nombre_archivo_cnv    = archivo_cnv.name
                        nombre_archivo_cnv    = nombre_archivo_cnv.replace('.cnv','.btl')
                                           
                        if nombre_archivo_cnv == nombre_archivo_btl:
                                                                                    
                            datos_archivo_cnv = archivo_cnv.getvalue().decode('ISO-8859-1').splitlines() 
                                          
                            datos_perfil,df_perfiles,datos_muestreo_perfil = FUNCIONES_LECTURA.lectura_archivo_perfiles(datos_archivo_cnv)
                                                                                                
                            # Define el nombre del perfil
                            nombre_perfil = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(datos_muestreo_perfil['cast_muestreo'].iloc[0])
                            
                            # Conecta con la base de datos
                            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                            cursor = conn.cursor() 
                            
                            # Obtén el identificador del perfil en la base de datos
                            instruccion_sql = '''INSERT INTO perfiles_verticales (nombre_perfil,estacion,salida_mar,num_cast,fecha_perfil,hora_perfil,longitud_muestreo,latitud_muestreo)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (estacion,fecha_perfil,num_cast) DO NOTHING;''' 
                            
                            nombre_perfil = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E' + str(nombre_estacion) + '_C' + str(datos_muestreo_perfil['cast_muestreo'].iloc[0])
                            
                            conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                            cursor = conn.cursor()    
                            cursor.execute(instruccion_sql,(nombre_perfil,int(id_estacion),int(id_salida),int(datos_muestreo_perfil['cast_muestreo'].iloc[0]),datos_muestreo_perfil['fecha_muestreo'].iloc[0],datos_muestreo_perfil['hora_muestreo'].iloc[0],datos_muestreo_perfil['lon_muestreo'].iloc[0],datos_muestreo_perfil['lat_muestreo'].iloc[0]))
                            conn.commit() 
        
                            instruccion_sql = "SELECT perfil FROM perfiles_verticales WHERE nombre_perfil = '" + nombre_perfil + "';" 
                            cursor = conn.cursor()    
                            cursor.execute(instruccion_sql)
                            id_perfil =cursor.fetchone()[0]
                            conn.commit()       
                            
                            cursor.close()
                            conn.close() 
                            
                            df_perfiles['perfil'] = int(id_perfil)
                                                        
                            FUNCIONES_PROCESADO.inserta_datos(df_perfiles,'perfil',direccion_host,base_datos,usuario,contrasena,puerto)
               
                                
                                
                            if nombre_estacion == '2' and programa_seleccionado == 'RADIAL CORUÑA' :  # Estacion 2 del programa radiales, añadir muestreo correspondiente a la botella en superficie

                                 # Genera dataframe con el muestreo de la estacion 2
                                 pres_min                             = min(datos_perfil['presion_ctd'])
                                 df_temp                              = datos_perfil[datos_perfil['presion_ctd']==pres_min]
                                 
                                 # Elimina la fila correspondiente al comienzo del descenso
                                 if df_temp.shape[0] > 1:
                                    df_botella = df_temp.drop([0])
                                 else:
                                    df_botella = df_temp

                                 # Asigna los datos correspondientes
                                 df_botella['latitud']                = datos_muestreo_perfil['lat_muestreo'].iloc[0]
                                 df_botella['longitud']               = datos_muestreo_perfil['lon_muestreo'].iloc[0]
                                 df_botella['prof_referencia']        = 0
                                 df_botella['fecha_muestreo']         = datos_muestreo_perfil['fecha_muestreo'].iloc[0]
                                 
                                 
                                 df_botella = df_botella.drop(columns = ['c0S/m','flag'])
                                 try:
                                     df_botella = df_botella.drop(columns = ['sbeox0V','sbeox0ML/L'])
                                     df_botella['oxigeno_ctd_qf']   = 1
                                 except:
                                     pass   
                                 
                                 id_estacion                          = tabla_estaciones_programa['id_estacion'][tabla_estaciones_programa['nombre_estacion']==str(nombre_estacion)].iloc[0]
                                 df_botella['id_estacion_temp']       = int(id_estacion) 
                                 df_botella['id_salida']              = id_salida 
                                 df_botella['nombre_muestreo']        = abreviatura_programa + '_' + (datos_muestreo_perfil['fecha_muestreo'].iloc[0]).strftime("%Y%m%d") + '_E2_P0' 
                                 
                                 df_botella['programa']               = id_programa    
                                 df_botella['num_cast']               = datos_muestreo_perfil['cast_muestreo'].iloc[0]
                                 
                                 # Añade botella (7) y hora de muestreo (nulas) para evitar errores en el procesado
                                 df_botella['botella']                = 7
                                 df_botella['hora_muestreo']          = None
                          
                                 # Añade qf 
                                 df_botella['temperatura_ctd_qf']     = 1
                                 df_botella['salinidad_ctd_qf']       = 1
                                 df_botella['fluorescencia_ctd_qf']   = 1
                                 df_botella['par_ctd_qf']             = 1
                            
                          
                                 df_botella                           = FUNCIONES_PROCESADO.evalua_registros(df_botella,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
 
                                 FUNCIONES_PROCESADO.inserta_datos(df_botella,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)

                texto_exito = 'Estación ' + nombre_estacion + ' procesada correctamente. Información subida a la base de datos'
                st.success(texto_exito)                            
             

            

    # Control de calidad 
    if tipo_accion == acciones[1]: 
    
        
        st.subheader('Control de calidad de datos procedentes de botellas')    
    
        # Define las variables a utilizar
        variables_procesado    = ['Temperatura','Salinidad','PAR','Fluorescencia','O2(CTD)']    
        variables_procesado_bd = ['temperatura_ctd','salinidad_ctd','par_ctd','fluorescencia_ctd','oxigeno_ctd']
        variables_unidades     = ['ºC','psu','\u03BCE/m2.s1','\u03BCg/kg','\u03BCmol/kg']

        # Toma los datos de la caché    
        df_muestreos,df_estaciones,df_datos_discretos,df_salidas,df_programas,df_indices_calidad = carga_datos_entrada_archivo_roseta()
        
        # Mantén sólo las salidas de radiales
        id_radiales   = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
        df_salidas  = df_salidas[df_salidas['programa']==int(id_radiales)]
        
        # Combina la información de muestreos y salidas en un único dataframe 
        df_muestreos          = df_muestreos.rename(columns={"salida_mar": "id_salida"}) # Para igualar los nombres de columnas                                               
        df_muestreos          = pandas.merge(df_muestreos, df_salidas, on="id_salida")
        df_muestreos          = df_muestreos.rename(columns={"id_salida": "salida_mar"}) # Deshaz el cambio de nombre
                         
        # compón un dataframe con la información de muestreo y datos biogeoquímicos                                            
        df_datos_disponibles  = pandas.merge(df_datos_discretos, df_muestreos, on="muestreo")
         
        # Añade columna con información del año
        df_datos_disponibles['año'] = pandas.DatetimeIndex(df_datos_disponibles['fecha_muestreo']).year
               
        # Borra los dataframes que ya no hagan falta para ahorrar memoria
        del(df_datos_discretos,df_muestreos)
        
        # procesa ese dataframe
        io_control_calidad = 1
        indice_programa,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada,salida_seleccionada = FUNCIONES_AUXILIARES.menu_seleccion(df_datos_disponibles,variables_procesado,variables_procesado_bd,io_control_calidad,df_salidas,df_estaciones,df_programas)
                                                   
        # Recupera el nombre "completo" de la variable y sus unidades
        indice_variable          = variables_procesado_bd.index(variable_seleccionada)
        nombre_completo_variable = variables_procesado[indice_variable] 
        unidades_variable        = variables_unidades[indice_variable]
                        
                                                        
        # Selecciona los datos correspondientes al programa, estación, salida y cast seleccionados
        datos_procesados     = df_datos_disponibles[(df_datos_disponibles["programa"] == indice_programa) & (df_datos_disponibles["estacion"] == indice_estacion) & (df_datos_disponibles["salida_mar"] == indice_salida) & (df_datos_disponibles["num_cast"] == cast_seleccionado)]

        df_datos_disponibles = df_datos_disponibles[(df_datos_disponibles["programa"] == indice_programa) & (df_datos_disponibles["estacion"] == indice_estacion)]
        
        FUNCIONES_PROCESADO.control_calidad_biogeoquimica(datos_procesados,df_datos_disponibles,variable_seleccionada,nombre_completo_variable,unidades_variable,df_indices_calidad,meses_offset)









# ###############################################################################
# ##################### PÁGINA DE CONSULTA DE DATOS DE BOTELLAS #################
# ###############################################################################    


def consulta_datos():
   
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['Consultar datos de botellas', 'Consultar datos de perfiles']
    tipo_accion  = st.sidebar.radio("Indicar la acción a realizar",acciones)
    

    # Consulta datos de botellas
    if tipo_accion == acciones[0]: 
           
        FUNCIONES_AUXILIARES.consulta_botellas()


    # Consulta datos de perfiles
    if tipo_accion == acciones[1]:

        FUNCIONES_AUXILIARES.consulta_perfiles()




        
        
        
###############################################################################
############ PÁGINA DE PROCESADO DE INFORMACION DE NUTRIENTES #################
###############################################################################    


def procesado_nutrientes():
    
    # Función para cargar en caché los datos a utilizar
    @st.cache_data(ttl=600,show_spinner="Cargando información de la base de datos")
    def carga_datos_procesado_nutrientes():
        conn                      = init_connection()
        df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
        df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
        df_datos_discretos        = psql.read_sql('SELECT * FROM datos_discretos', conn)
        df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas              = psql.read_sql('SELECT * FROM programas', conn)
        df_indices_calidad        = psql.read_sql('SELECT * FROM indices_calidad', conn)
        df_rmns_bajos             = psql.read_sql('SELECT * FROM rmn_bajo_nutrientes', conn)
        df_rmns_altos             = psql.read_sql('SELECT * FROM rmn_alto_nutrientes', conn)
        conn.close()
        return df_muestreos,df_estaciones,df_datos_discretos,df_salidas,df_programas,df_indices_calidad,df_rmns_bajos,df_rmns_altos
        


     
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port
    
   
    df_muestreos,df_estaciones,df_datos_discretos,df_salidas,df_programas,df_indices_calidad,df_rmns_bajos,df_rmns_altos = carga_datos_procesado_nutrientes()

    # Combina la información de muestreos y salidas en un único dataframe 
    df_salidas            = df_salidas.rename(columns={"id_salida": "salida_mar"}) # Para igualar los nombres de columnas                                               
    df_muestreos          = pandas.merge(df_muestreos, df_salidas, on="salida_mar")
                         
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['Procesar salidas del AA','Modificar datos procesados','Realizar control de calidad de datos disponibles']
    tipo_accion  = st.sidebar.radio("Indicar la acción a realizar",acciones)
 
    # Define los vectores con las variables a procesar
    variables_procesado    = ['TON','Nitrato','Nitrito','Silicato','Fosfato']    
    variables_procesado_bd = ['ton','nitrato','nitrito','silicato','fosfato']
    variables_unidades     = ['\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg','\u03BCmol/kg']
    
    
    # Define unos valores de referencia 
    df_referencia        = pandas.DataFrame(columns = variables_procesado_bd,index = [0])
    df_referencia.loc[0] = [float(0),float(0),float(0),float(0),float(0)]
    
    # Añade salidas del AA
    if tipo_accion == acciones[0]:
        
        st.subheader('Procesado de datos de nutrientes')
        
        canales_autoanalizador = ['ton','nitrito','silicato','fosfato']
    
        with st.form("Formulario", clear_on_submit=False):
              
            # Despliega un formulario para subir los archivos del AA y las referencias
            col1, col2,col3,col4 = st.columns(4,gap="small")
            with col1:
                temperatura_laboratorio = st.number_input('Temperatura laboratorio:',value=20.5)
            with col2:
                rendimiento_columna     = st.number_input('Rendimiento columna:',value=float(100),min_value=float(0),max_value=float(100))
            with col3:            
                rmn_elegida_bajo             = st.selectbox("Selecciona RMN **BAJO**", (df_rmns_bajos['nombre_rmn']))
                df_referencias_bajas    = df_rmns_bajos[df_rmns_bajos['nombre_rmn']==rmn_elegida_bajo]
            with col4:            
                rmn_elegida_alto             = st.selectbox("Selecciona RMN **ALTO**", (df_rmns_altos['nombre_rmn']))
                df_referencias_altas    = df_rmns_altos[df_rmns_altos['nombre_rmn']==rmn_elegida_alto]
            

            
            archivo_AA                  = st.file_uploader("Arrastra o selecciona los archivos del AA", accept_multiple_files=False)
            
            io_add_data                 = st.checkbox('Añadir datos procesados a la base de datos',value=True)
                            
            io_envio                    = st.form_submit_button("Procesar el archivo subido")        
        
        if archivo_AA is not None and io_envio is True:
    
        
            # Lectura del archivo con los resultados del AA
            datos_AA              = pandas.read_excel(archivo_AA,skiprows=15)            
            datos_AA              = datos_AA.rename(columns={"Results 1":canales_autoanalizador[0],"Results 2":canales_autoanalizador[1],"Results 3":canales_autoanalizador[2],"Results 4":canales_autoanalizador[3]})
                  
            # Identifica qué canales/variables se han procesado
            variables_procesadas = datos_AA.columns.tolist()
            variables_run        = list(set(variables_procesadas).intersection(variables_procesado_bd))
            
            # Añade la información de salinidad en aquellas muestras que tienen un muestreo asociado                                            
            df_datos_disponibles  = pandas.merge(df_datos_discretos, df_muestreos, on="muestreo")            
                        
            # Adapta el nombre de las sw
            for idato in range(datos_AA.shape[0]):
                if datos_AA['Sample ID'].iloc[idato][0:2].lower()=='sw':
                   datos_AA['Sample ID'].iloc[idato] ='sw' 
            
            # Encuentra las posiciones de las referencias de sw
            indices_referencias = numpy.asarray(datos_AA['Peak Number'][datos_AA['Sample ID']=='sw']) - 1
            # Agrupa en dos tandas, las iniciales y las finales
            spl          = [0]+[i for i in range(1,len(indices_referencias)) if indices_referencias[i]-indices_referencias[i-1]>1]+[None]
            listado_refs = [indices_referencias[b:e] for (b, e) in [(spl[i-1],spl[i]) for i in range(1,len(spl))]]

            ref_inicial        = listado_refs[0][-1] + 1
            ref_final          = listado_refs[1][0]
            
            # Encuentra la salinidad de cada muestra
            datos_AA['salinidad']     = numpy.ones(datos_AA.shape[0])
            datos_AA['io_procesado']  = None
            for idato in range(ref_inicial,ref_final):
                if datos_AA['Cup Type'].iloc[idato] == 'SAMP':
     
                    id_temp = df_datos_disponibles['muestreo'][df_datos_disponibles['id_externo']==datos_AA['Sample ID'].iloc[idato]]
                        
                    if len(id_temp) > 0:
                        datos_AA['salinidad'].iloc[idato]     = df_datos_disponibles['salinidad_ctd'][df_datos_disponibles['muestreo']==id_temp.iloc[0]]
                        datos_AA['io_procesado'].iloc[idato]  = 1
                    else:
                        texto_error = 'La muestra ' + datos_AA['Sample ID'].iloc[idato] + ' no está inlcluida en la base de datos y no ha sido procesada'
                        st.warning(texto_error, icon="⚠️")                        
       
            # comprobación por si no hay ningún dato a procesar
            if datos_AA['io_procesado'].isnull().all():
                texto_error = "Ninguna de las muestras analizadas se corresponde con muestreos incluidos en la base de datos"
                st.warning(texto_error, icon="⚠️")          
   
            else:
                
            # En caso contrario procesa los datos
                        
                # Aplica la corrección de deriva (DRIFT)                 
                datos_corregidos = FUNCIONES_PROCESADO.correccion_drift(datos_AA,df_referencias_altas,df_referencias_bajas,variables_run,rendimiento_columna,temperatura_laboratorio)
                                            
                # Calcula el NO3 como diferencia entre el TON y el NO2 (sólo si se han procesado estos dos canales)
                if 'ton' in variables_run and 'nitrito' in variables_run:
                    datos_corregidos['nitrato'] = datos_corregidos['ton'] - datos_corregidos['nitrito']
                    datos_corregidos['nitrato'][datos_corregidos['nitrato']<0]   = 0
                    
                    # vuelvo a calcular el TON como NO3+NO2, por si hubiese corregido valores nulos
                    datos_corregidos['ton'] = datos_corregidos['nitrato'] + datos_corregidos['nitrito']
                    
                    # añade nitrato a variables procesadas (para redondear decimales y añadir qf)
                    variables_run = variables_run + ['nitrato']
            
                # Añade informacion de RMNs, temperaturas y rendimiento
                datos_corregidos['rto_columna_procesado']  = rendimiento_columna
                datos_corregidos['temp_lab_procesado']     = temperatura_laboratorio
                datos_corregidos['rmn_bajo_procesado']     = int(df_referencias_bajas['id_rmn'].iloc[0])
                datos_corregidos['rmn_alto_procesado']     = int(df_referencias_altas['id_rmn'].iloc[0])
                
                texto_exito = 'Muestreos disponibles procesados correctamente'
                st.success(texto_exito)
                
                
                # Añade información de la base de datos (muestreo, biogeoquimica y fisica)
                datos_corregidos = pandas.merge(datos_corregidos, df_muestreos, on="id_externo") # Esta unión elimina los registros que NO son muestras
                
                variables_elimina       = variables_procesado_bd + ['rto_columna_procesado','temp_lab_procesado','rmn_bajo_procesado','rmn_alto_procesado']
                df_datos_biogeoquimicos = df_datos_discretos.drop(columns=variables_elimina) # Para eliminar las columnas previas con datos de nutrientes

                datos_corregidos = pandas.merge(datos_corregidos, df_datos_biogeoquimicos, on="muestreo",how='left')
                
                # Reduce los decimales y asigna QF a los datos
                variables_run_qf = []
                for ivariable_procesada in range(len(variables_run)):
                        
                    #reduce los decimales 
                    datos_corregidos[variables_run[ivariable_procesada]]=round(datos_corregidos[variables_run[ivariable_procesada]],3)
                        
                    # Añade qf a los datos, asignando a las variables procesadas un qf de valor 1 (no evaluado)
                    variables_run_qf                                        = variables_run_qf + [variables_run[ivariable_procesada] + '_qf']
                    datos_corregidos[variables_run_qf[ivariable_procesada]] = numpy.ones(datos_corregidos.shape[0],dtype=int)
  
                
                variables_exporta =  variables_procesado_bd + ['rto_columna_procesado','temp_lab_procesado','rmn_bajo_procesado','rmn_alto_procesado','muestreo']
                datos_exporta = datos_corregidos[variables_exporta]
                
                #st.dataframe(datos_exporta)
                #datos_exporta['rto_columna_procesado']  = None
                #datos_exporta['temp_lab_procesado']     = None
                #datos_exporta['rmn_bajo_procesado']     = None
                #datos_exporta['rmn_alto_procesado']     = None
                
                # Añade los datos a la base de datos si se seleccionó esta opción                        
                if io_add_data is True:
                                        
                    FUNCIONES_PROCESADO.inserta_datos(datos_exporta,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)
                    #FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)
                    
                    
                    texto_exito = 'Información introducida correctamente en la base de datos'
                    st.success(texto_exito)

                # Añade nombre de la estacion
                df_estaciones = df_estaciones.rename(columns={"id_estacion": "estacion"})
                datos_corregidos  = pandas.merge(datos_corregidos, df_estaciones, on="estacion")

                # Descarga los datos como una hoja Excel        
                listado_columnas        = ['nombre_muestreo','id_externo','fecha_muestreo','hora_muestreo','nombre_estacion','botella','presion_ctd','salinidad_ctd'] + variables_run + variables_run_qf
                datos_corregidos        = datos_corregidos[listado_columnas]
      
                # Botón para descargar la información como Excel
                nombre_archivo =  'PROCESADO_' + archivo_AA.name[0:-5] + '.xlsx'
                       
                output = BytesIO()
                writer = pandas.ExcelWriter(output, engine='xlsxwriter')
                datos_excel = datos_corregidos.to_excel(writer, index=False, sheet_name='DATOS')
                writer.close()
                datos_excel = output.getvalue()
            
                st.download_button(
                    label="DESCARGA EXCEL CON LOS DATOS PROCESADOS",
                    data=datos_excel,
                    file_name=nombre_archivo,
                    help= 'Descarga un archivo .xlsx con los datos procesados',
                    mime="application/vnd.ms-excel"
                )              
       
                st.cache_data.clear()
                              






    # Añade manualmente resultados del procesado 
    if tipo_accion == acciones[1]:
        
        st.subheader('Inserción de datos de nutrientes')
        
        FUNCIONES_AUXILIARES.inserta_datos_biogeoquimicos(df_muestreos,df_datos_biogeoquimicos,variables_procesado,variables_procesado_bd,df_referencia)

        

        
    # control de calidad de salidas previamente disponibles
    if tipo_accion == acciones[2]: 
        
        st.subheader('Control de calidad de datos procedentes de botellas')    
            
        # Toma los datos de la caché    
        df_muestreos,df_estaciones,df_datos_biogeoquimicos,df_datos_fisicos,df_salidas,df_programas,df_indices_calidad,df_rmns = carga_datos_procesado_nutrientes()
                
        # # Mantén sólo las salidas de radiales
        # id_radiales   = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
        # df_salidas  = df_salidas[df_salidas['programa']==int(id_radiales)]
        
        # Combina la información de muestreos y salidas en un único dataframe 
        df_muestreos          = df_muestreos.rename(columns={"salida_mar": "id_salida"}) # Para igualar los nombres de columnas                                               
        df_muestreos          = pandas.merge(df_muestreos, df_salidas, on="id_salida")
        df_muestreos          = df_muestreos.rename(columns={"id_salida": "salida_mar"}) # Deshaz el cambio de nombre
                         
        # compón un dataframe con la información de muestreo y datos disponibles                                            
        df_datos_disponibles  = pandas.merge(df_datos_biogeoquimicos, df_muestreos, on="muestreo")
        df_datos_disponibles  = pandas.merge(df_datos_disponibles, df_datos_fisicos, on="muestreo")
         
        # Añade columna con información del año
        df_datos_disponibles['año'] = pandas.DatetimeIndex(df_datos_disponibles['fecha_muestreo']).year
        
        # Borra los dataframes que ya no hagan falta para ahorrar memoria
        del(df_datos_biogeoquimicos,df_datos_fisicos,df_muestreos)
        
        # procesa ese dataframe
        io_control_calidad = 1
        indice_programa,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada,salida_seleccionada = FUNCIONES_AUXILIARES.menu_seleccion(df_datos_disponibles,variables_procesado,variables_procesado_bd,io_control_calidad,df_salidas,df_estaciones,df_programas)
                                                   
        # Recupera el nombre "completo" de la variable y sus unidades
        indice_variable          = variables_procesado_bd.index(variable_seleccionada)
        nombre_completo_variable = variables_procesado[indice_variable] 
        unidades_variable        = variables_unidades[indice_variable]
                                                                      
        # Selecciona los datos correspondientes al programa, estación, salida y cast seleccionados
        datos_procesados     = df_datos_disponibles[(df_datos_disponibles["programa"] == indice_programa) & (df_datos_disponibles["estacion"] == indice_estacion) & (df_datos_disponibles["salida_mar"] == indice_salida) & (df_datos_disponibles["num_cast"] == cast_seleccionado)]

        df_datos_disponibles = df_datos_disponibles[(df_datos_disponibles["programa"] == indice_programa) & (df_datos_disponibles["estacion"] == indice_estacion)]
                
        if not datos_procesados[variable_seleccionada].isnull().all():         
  
            FUNCIONES_PROCESADO.control_calidad_biogeoquimica(datos_procesados,df_datos_disponibles,variable_seleccionada,nombre_completo_variable,unidades_variable,df_indices_calidad,meses_offset)

        else:
            
            texto_error = "La base de datos no contiene información para la variable, salida y estación seleccionadas"
            st.warning(texto_error, icon="⚠️")

        
        
        # st.subheader('Control de calidad de datos de nutrientes')

        # # compón un dataframe con la información de muestreo y datos biogeoquímicos
        # df_muestreos          = df_muestreos.rename(columns={"id_muestreo": "muestreo"}) # Para igualar los nombres de columnas                                               
        # df_datos_disponibles  = pandas.merge(df_datos_biogeoquimicos, df_muestreos, on="muestreo")
        
        # # Añade columna con información del año
        # df_datos_disponibles['año']                = numpy.zeros(df_datos_disponibles.shape[0],dtype=int)
        # for idato in range(df_datos_disponibles.shape[0]):
        #     df_datos_disponibles['año'].iloc[idato] = (df_datos_disponibles['fecha_muestreo'].iloc[idato]).year
        
        
        # # procesa ese dataframe
        # FUNCIONES_PROCESADO.control_calidad_biogeoquimica(df_datos_disponibles,variables_procesado,variables_procesado_bd,variables_unidades)

        

            
            
###############################################################################
################## PÁGINA DE PROCESADO DE INFORMACION QUIMICA #################
###############################################################################    


def entrada_datos_laboratorio():
        
    st.subheader('Procesado de variables analizadas en laboratorio')
    
    
    
    # Función para cargar en caché los datos a utilizar
    @st.cache_data(ttl=600,show_spinner="Cargando información de la base de datos")
    def carga_datos_entrada_laboratorio():
        conn                    = init_connection()
        df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
        df_datos_discretos      = psql.read_sql('SELECT * FROM datos_discretos', conn)
        df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
        df_programas            = psql.read_sql('SELECT * FROM programas', conn)
        df_indices_calidad      = psql.read_sql('SELECT * FROM indices_calidad', conn)
        df_metodo_ph            = psql.read_sql('SELECT * FROM metodo_ph', conn)
        conn.close()
        return df_muestreos,df_datos_discretos,df_salidas,df_estaciones,df_programas,df_indices_calidad,df_metodo_ph
        

    df_muestreos,df_datos_discretos,df_salidas,df_estaciones,df_programas,df_indices_calidad,df_metodo_ph = carga_datos_entrada_laboratorio()

    # Mantén sólo las salidas de radiales
    id_radiales   = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].tolist()[0]
    df_salidas  = df_salidas[df_salidas['programa']==int(id_radiales)]

    # Combina la información de muestreos y salidas en un único dataframe 
    df_salidas            = df_salidas.rename(columns={"id_salida": "salida_mar"}) # Para igualar los nombres de columnas                                               
    df_muestreos          = pandas.merge(df_muestreos, df_salidas, on="salida_mar")
    df_salidas            = df_salidas.rename(columns={"salida_mar": "id_salida"})        
                      
    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['Añadir o modificar datos procesados', 'Realizar control de calidad de datos disponibles']
    tipo_accion  = st.sidebar.radio("Indicar la acción a realizar",acciones)
 
    variables_procesado    = ['TOC','pH','Alcalinidad','Oxígeno (Método Winkler)']    
    variables_procesado_bd = ['toc','ph','alcalinidad','oxigeno_wk']
    variables_unidades     = ['µmol/kg',' ','\u03BCmol/kg','\u03BCmol/kg']
    
    # Define unos valores de referencia 
    df_referencia        = pandas.DataFrame(columns = ['toc','ph', 'alcalinidad', 'oxigeno_wk'],index = [0])
    df_referencia.loc[0] = [0.0,8.1,200.0,200.0]
    
    # Añade nuevos datos obtenidos en laboratorio
    if tipo_accion == acciones[0]:
        
        FUNCIONES_AUXILIARES.inserta_datos_biogeoquimicos(df_muestreos,df_datos_discretos,variables_procesado,variables_procesado_bd,variables_unidades,df_referencia,df_salidas,df_estaciones,df_programas,df_indices_calidad,df_metodo_ph)

    
    # Realiza control de calidad
    if tipo_accion == acciones[1]:
        
        # Añade columna con información del año
        df_muestreos['año'] = pandas.DatetimeIndex(df_muestreos['fecha_muestreo']).year

        
        # Despliega menú de selección del programa, año, salida, estación, cast y variable                 
        io_control_calidad = 1
        indice_programa,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada,salida_seleccionada = FUNCIONES_AUXILIARES.menu_seleccion(df_muestreos,variables_procesado,variables_procesado_bd,io_control_calidad,df_salidas,df_estaciones,df_programas)
  
        df_datos             = pandas.merge(df_muestreos, df_datos_discretos, on="muestreo")
        
        df_datos_disponibles = df_datos[(df_datos["programa"] == indice_programa) & (df_datos["estacion"] == indice_estacion)]
                        
        datos_procesados     = df_datos_disponibles[(df_datos_disponibles["salida_mar"] == indice_salida) & (df_datos_disponibles["num_cast"] == cast_seleccionado)]
      
        if not datos_procesados[variable_seleccionada].isnull().all():         
  
            indice_seleccion               = variables_procesado_bd.index(variable_seleccionada)
            variable_seleccionada_nombre   = variables_procesado[indice_seleccion]
            variable_seleccionada_unidades = variables_unidades[indice_seleccion]          
  
            FUNCIONES_PROCESADO.control_calidad_biogeoquimica(datos_procesados,df_datos_disponibles,variable_seleccionada,variable_seleccionada_nombre,variable_seleccionada_unidades,df_indices_calidad,meses_offset)

        else:

            texto_aviso = 'La base de datos no contiene información de ' + variable_seleccionada + ' correspondientes a la salida ' + salida_seleccionada 
            st.warning(texto_aviso)
 
# ###############################################################################
# ################## PÁGINA DE ENTRADA DE ESTADILLOS #################
# ###############################################################################    



def entrada_datos_excel():
    
    st.subheader('Portal de entrada de datos')
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port
    
    # Recupera la informacion de la cache 
    #df_programas,variables_bd = carga_datos_entrada_datos_excel()
    conn                      = init_connection()
    df_programas              = psql.read_sql('SELECT * FROM programas', conn)
    variables_bd              = psql.read_sql('SELECT * FROM variables_procesado', conn)
    conn.close()   
    
    
       
    
    # Despliega menús de selección de la variable, salida y la estación a controlar                
    listado_tipos_salida  = ['SEMANAL','MENSUAL','ANUAL','PUNTUAL']
    with st.form("Formulario", clear_on_submit=False):
    
        col1, col2 = st.columns(2,gap="small")
        with col1: 
            programa_seleccionado = st.selectbox('Programa',(df_programas['nombre_programa']))
    
        with col2: 
            tipo_salida           = st.selectbox('Tipo de salida',(listado_tipos_salida))
            
        nombre_entrada      = st.text_input('Nombre del muestreo **(solo para datos del programa "Otros")**')
            
        archivo_datos       = st.file_uploader("Arrastra o selecciona el archivo con los datos a importar", accept_multiple_files=False)
            
        io_envio            = st.form_submit_button("Procesar el archivo subido")
        
    col1, col2= st.columns(2,gap="small")

    with col1:
        
        st.markdown('Consulta las instrucciones para añadir información a la base de datos.')

    with col2:

        archivo_metadatos     = 'DATOS/Instrucciones_entrada.pdf'
        with open(archivo_metadatos, "rb") as pdf_file:
            PDFbyte = pdf_file.read()
        
        st.download_button(label="DESCARGA INSTRUCCIONES",
                            data=PDFbyte,
                            file_name="Instrucciones.pdf",
                            mime='application/octet-stream')
    
   
    
        
    
    if programa_seleccionado == 'OTROS':
        if len(nombre_entrada) <2:
            st.warning('El nombre del muestreo no puede ser nulo en datos de programa "OTROS"')
            st.stop()
    else:
        nombre_entrada = programa_seleccionado  
    
 
    if archivo_datos is not None and io_envio is True:
        
        df_datos_importacion  = pandas.read_excel(archivo_datos) 
        
        # Identifica las variables que contiene el archivo
        variables_archivo    = df_datos_importacion.columns.tolist()
        variables_discretas  = list(set(variables_bd['variables']).intersection(variables_archivo))

                                
        # Corrige el formato de las fechas
        for idato in range(df_datos_importacion.shape[0]):
            df_datos_importacion['fecha_muestreo'].iloc[idato] = (df_datos_importacion['fecha_muestreo'].iloc[idato]).date()           
            if df_datos_importacion['fecha_muestreo'].iloc[idato]:
                if 'hora_muestreo' in variables_archivo and isinstance(df_datos_importacion['hora_muestreo'].iloc[idato], str):
                    df_datos_importacion['hora_muestreo'].iloc[idato] = datetime.datetime.strptime(df_datos_importacion['hora_muestreo'].iloc[idato], '%H:%M:%S').time()

        # Cambia el nombre del identificador 
        try:
            df_datos_importacion = df_datos_importacion.rename(columns={"ID": "id_externo"})
        except:
            texto_aviso = "Los datos importados no contienen identificador."
            st.warning(texto_aviso, icon="⚠️")

        # Si los datos incluyen informacion del tubo de nutrientes, cambiar el nombre a texto 
        if 'tubo_nutrientes' in variables_discretas:
            for idato in range(df_datos_importacion.shape[0]):
                try:
                    df_datos_importacion['tubo_nutrientes'].iloc[idato] = str(int(df_datos_importacion['tubo_nutrientes'].iloc[idato]))
                except:
                    pass

        # Realiza un control de calidad primario a los datos importados   
        datos_corregidos,textos_aviso   = FUNCIONES_PROCESADO.control_calidad(df_datos_importacion)  

        # Recupera el identificador del programa de muestreo
        id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_seleccionado,direccion_host,base_datos,usuario,contrasena,puerto)
                
        with st.spinner('Asignando la estación y salida al mar de cada medida'):
            # Encuentra la estación asociada a cada registro
            datos_corregidos = FUNCIONES_PROCESADO.evalua_estaciones(datos_corregidos,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)

            # Encuentra las salidas al mar correspondientes  
            datos_corregidos = FUNCIONES_PROCESADO.evalua_salidas(datos_corregidos,id_programa,nombre_entrada,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
     
        # Encuentra el identificador asociado a cada registro
        with st.spinner('Asignando el registro correspondiente a cada medida'):
            datos_corregidos = FUNCIONES_PROCESADO.evalua_registros(datos_corregidos,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
             
        # Añade datos físicos
        if len(variables_discretas)>0:
                            
            with st.spinner('Añadiendo datos discretos'):
                
                FUNCIONES_PROCESADO.inserta_datos(datos_corregidos,'discreto',direccion_host,base_datos,usuario,contrasena,puerto)
                
        texto_exito = 'Datos del archivo ' + archivo_datos.name + ' añadidos correctamente a la base de datos'
        st.success(texto_exito)
        







# ###############################################################################
# #### PÁGINA DE ENTRADA DE RMNs UTILIZADOS EN EL PROCESADO DE NUTRIENTES #######
# ###############################################################################  

def referencias_nutrientes():
    
    from sqlalchemy import create_engine
    
    # Recupera los parámetros de la conexión a partir de los "secrets" de la aplicación
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port

    # Despliega un botón lateral para seleccionar el tipo de información a mostrar       
    acciones     = ['RMN Altos', 'RMN Bajos']
    tipo_accion  = st.sidebar.radio("Seleccionar tipo de RMN ",acciones)    
    
    if tipo_accion == acciones[0]:
        nombre_tabla     = 'rmn_alto_nutrientes'
        texto_formulario = 'Nombre del RMN **ALTO**'
        cabecera         = 'RMNs Altos'
        
        
    if tipo_accion == acciones[1]:
        nombre_tabla     = 'rmn_bajo_nutrientes'
        texto_formulario = 'Nombre del RMN **BAJO**'
        cabecera         = 'RMNs Bajos'

      
    st.subheader(cabecera)
    
    # Despliega un formulario para introducir los datos de las muestras que se están analizando
    with st.form("Formulario seleccion"):
    
        nombre_muestras = st.text_input(texto_formulario, value="")
        
        col1, col2, col3= st.columns(3,gap="small")
        with col1:
            salinidad = st.number_input('Salinidad (PSU):',value=float(35))
            ton       = st.number_input('Nitrógeno total (µmol/L):',value=float(15))

            
        with col2:
            nitrito   = st.number_input('Nitrito  (µmol/L):',value=float(0.5))
            silicato  = st.number_input('Silicato (µmol/L):',value=float(8.5))
 
        with col3:
            fosfato   = st.number_input('Fosfato  (µmol/L):',value=float(1))   

        observaciones = st.text_input('**Observaciones**', value="")
        
        if len(observaciones) < 1:
            observaciones = None

        io_envio = st.form_submit_button('Actualizar la tabla de RMNs') 

        if io_envio: 
            
            # Comprueba valores
            if len(nombre_muestras) == 0 or salinidad is None or ton is None or nitrito is None or silicato is None or fosfato is None:
                texto_error = 'IMPORTANTE. Los campos de nombre, TON, nitrito, silicato y fosfato no pueden ser nulos' 
                st.warning(texto_error, icon="⚠️")
                
            else:
            
                with st.spinner('Añadiendo RMN a la base de datos'):
                
                    instruccion_sql = 'INSERT INTO ' + nombre_tabla + ' (nombre_rmn,salinidad,ton,nitrito,silicato,fosfato,observaciones) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (nombre_rmn) DO UPDATE SET (salinidad,ton,nitrito,silicato,fosfato,observaciones) = ROW(EXCLUDED.salinidad,EXCLUDED.ton,EXCLUDED.nitrito,EXCLUDED.silicato,EXCLUDED.fosfato,EXCLUDED.observaciones);'
                        
                    conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                    cursor = conn.cursor()
                    cursor.execute(instruccion_sql,(nombre_muestras,salinidad,ton,nitrito,silicato,fosfato,observaciones))
                    conn.commit()                 
                    cursor.close()
                    conn.close()
    
                    texto_exito = 'Referencia añadida o actualizada correctamente'
                    st.success(texto_exito)            
        
    # Recupera la tabla con los RMNs utilizados 
    con_engine = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql  = create_engine(con_engine)
    tabla_rmns_altos = psql.read_sql('SELECT * FROM rmn_alto_nutrientes', conn_psql)
    tabla_rmns_bajos = psql.read_sql('SELECT * FROM rmn_bajo_nutrientes', conn_psql)
    conn_psql.dispose()                

    st.markdown('RMNs incluidos en la base de datos')
    
    if tipo_accion == acciones[0]:
        st.dataframe(tabla_rmns_altos)
    if tipo_accion == acciones[1]:
        st.dataframe(tabla_rmns_bajos)
         


     
        
     
        
     
