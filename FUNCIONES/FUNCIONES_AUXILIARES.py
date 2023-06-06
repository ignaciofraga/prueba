# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 13:09:09 2022

@author: ifraga
"""

import streamlit as st
import psycopg2
import pandas.io.sql as psql
import numpy
from io import BytesIO
import pandas
from sqlalchemy import create_engine
import math
import datetime
import json

pandas.options.mode.chained_assignment = None

###############################################################################
###################### FUNCION CONEXIÓN #######################################
###############################################################################

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
#@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])



###############################################################################
####################### FUNCION LOG-IN ########################################
###############################################################################

def log_in():
    
    # Si no está autenticado (no existe username), procede a identificar al usuario
    if 'usuario' not in st.session_state:
        # Recupera las contraseñas y usuarios de la base de datos
        conn        = init_connection()
        df_usuarios = psql.read_sql('SELECT * FROM usuarios_app', conn)
        conn.close()   
        
        listado_usuarios    = df_usuarios['nombre_usuario']
        listado_contrasenas = df_usuarios['password']
    
        # Despliega un formulario para introducir el nombre de usuario y la contraseña
        formulario = st.empty()
        with formulario.form('formulario'):
            st.write("Introduzca sus datos de usuario y contraseña para acceder al servicio")
            col1, col2 = st.columns(2,gap="small")  
            with col1:
                usuario = st.selectbox('Selecciona el usuario',(listado_usuarios))
            with col2:
                contrasena = st.text_input("Contraseña", type="password")

            # Botón de envío para confirmar selección
            st.form_submit_button("Enviar")
        
        # comprueba si la contraseña introducida corresponde al usuario seleccionado    
        io_autorizado = 0 # por defecto no autorizado
        for iusuario_bd in range(len(listado_usuarios)):
            if usuario == listado_usuarios[iusuario_bd] and contrasena == listado_contrasenas[iusuario_bd]:
                io_autorizado = 1 # Autorizado!
                
        # Si el usuario está autorizado, devuelve "true" y añade al estado de la sesión el nombre de usuario
        if io_autorizado == 1:
           st.session_state['usuario'] = usuario
           formulario.empty()
           return True
        else:
           return False
       
    else:

        return True        
 
    
###############################################################################
####################### FUNCION ESTADO PROCESOS ########################################
###############################################################################
    
def estado_procesos(altura_tabla):
    
    # Recupera los muestreos almacenados 
    conn = init_connection()
    df_muestreos = psql.read_sql('SELECT * FROM procesado_actual_nutrientes', conn)
    conn.close()
    
    # Seleccionar los muestreos en curso como aquellos con io_estado = 1
    df_muestreos_curso = df_muestreos[df_muestreos['io_estado']==1]
    
    # Define una columna índice
    indices_dataframe            = numpy.arange(0,df_muestreos_curso.shape[0],1,dtype=int)
    df_muestreos_curso['indice'] = indices_dataframe
    df_muestreos_curso.set_index('indice',drop=True,append=False,inplace=True)

    if df_muestreos_curso.shape[0] > 0:
            
        # Elimina las columnas que no interesa mostrar
        df_muestreos_curso = df_muestreos_curso.drop(columns=['id_proceso','programa','io_estado','fecha_real_fin'])
    
        # Renombra las columnas
        df_muestreos_curso = df_muestreos_curso.rename(columns={'nombre_proceso':'Muestras','nombre_programa':'Programa','año':'Año','num_muestras':'Número muestras','fecha_inicio':'Inicio','fecha_estimada_fin':'Final estimado'})
    
        # Ajusta el formato de las fechas
        for idato in range(df_muestreos_curso.shape[0]):
            df_muestreos_curso['Inicio'][idato]         =  df_muestreos_curso['Inicio'][idato].strftime("%Y-%m-%d")
            df_muestreos_curso['Final estimado'][idato] =  df_muestreos_curso['Final estimado'][idato].strftime("%Y-%m-%d")
          
        # Muestra una tabla con los análisis en curso
        st.dataframe(df_muestreos_curso,use_container_width=True)

    else:
        
        texto_error = 'Actualmente no hay ninguna muestra en proceso.'
        st.warning(texto_error, icon="⚠️")         

    return df_muestreos_curso









#################################################################################
######## FUNCION PARA DESPLEGAR MENUS DE SELECCION DE SALIDA Y VARIABLE  ########
#################################################################################
def menu_seleccion(datos_procesados,variables_procesado,variables_procesado_bd,io_control_calidad,df_salidas,df_estaciones,df_programas):
    
    # Despliega menús de selección de la variable, salida y la estación a controlar                
    col1, col2, col3 = st.columns(3,gap="small")
    with col1: 
        
        listado_programas         = datos_procesados['programa'].unique()
        df_programas_muestreados  = df_programas[df_programas['id_programa'].isin(listado_programas)]
        programa_seleccionado     = st.selectbox('Programa',(df_programas_muestreados['nombre_programa']))
        indice_programa           = df_programas['id_programa'][df_programas['nombre_programa']==programa_seleccionado].iloc[0]

    with col2:
        
        df_prog_sel               = datos_procesados[datos_procesados['programa']==indice_programa]
        df_prog_sel               = df_prog_sel.sort_values('año',ascending=False)
        anhos_disponibles         = df_prog_sel['año'].unique()
        anho_seleccionado         = st.selectbox('Año',(anhos_disponibles))
        
        df_prog_anho_sel          = df_prog_sel[df_prog_sel['año']==anho_seleccionado]
        
        
    with col3:
        
        listado_tipos_salida      = datos_procesados['tipo_salida'].unique()
        tipo_salida_seleccionada  = st.selectbox('Tipo de salida',(listado_tipos_salida))
        
        df_prog_anho_tipo_sel     = df_prog_anho_sel[df_prog_anho_sel['tipo_salida']==tipo_salida_seleccionada]


        # test = df_prog_anho_sel
        # test = test.drop(columns=['variables_muestreadas'])
        # st.dataframe(test)

    col1, col2 = st.columns(2,gap="small")
    with col1: 

        listado_salidas           = df_prog_anho_tipo_sel['salida_mar'].unique()
        df_salidas_muestreadas    = df_salidas[df_salidas['id_salida'].isin(listado_salidas)]
        df_salidas_muestreadas    = df_salidas_muestreadas.sort_values('fecha_salida',ascending=False)
        salida_seleccionada       = st.selectbox('Salida',(df_salidas_muestreadas['nombre_salida']))
        indice_salida             = df_salidas['id_salida'][df_salidas['nombre_salida']==salida_seleccionada].iloc[0]

        df_prog_anho_sal_sel      = df_prog_anho_tipo_sel[df_prog_anho_tipo_sel['salida_mar']==indice_salida]

    with col2:

      
        listado_id_estaciones        = df_prog_anho_sal_sel['estacion'].unique() 
        df_estaciones_disponibles    = df_estaciones[df_estaciones['id_estacion'].isin(listado_id_estaciones)]

        estacion_seleccionada        = st.selectbox('Estación',(df_estaciones_disponibles['nombre_estacion']))
        indice_estacion              = df_estaciones_disponibles['id_estacion'][df_estaciones_disponibles['nombre_estacion']==estacion_seleccionada].iloc[0]
        
        df_prog_anho_sal_est_sel     = df_prog_anho_sal_sel[df_prog_anho_sal_sel['estacion']==indice_estacion]
    
    del(df_salidas,df_estaciones,df_programas)
    
    # Un poco diferente según se utilice el menú para control de calidad (con rango de meses) o no (sin él)
    if io_control_calidad == 1:
        col1, col2,col3 = st.columns(3,gap="small")
    else:
        col1, col2      = st.columns(2,gap="small")
    
    with col1: 
        
        listado_casts_estaciones  = df_prog_anho_sal_est_sel['num_cast'].unique() 
        listado_casts_estaciones  = listado_casts_estaciones.astype(int)
        cast_seleccionado         = st.selectbox('Cast',(listado_casts_estaciones))
        
        
    with col2: 

        variable_seleccionada        = st.selectbox('Variable',(variables_procesado))
        indice_variable_seleccionada = variables_procesado.index(variable_seleccionada)
        variable_seleccionada = variables_procesado_bd[indice_variable_seleccionada]



    if io_control_calidad == 1: 
        with col3:  
            meses_offset              = st.number_input('Intervalo meses:',value=1)
    else:
        meses_offset = 0
    

    return indice_programa,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada,salida_seleccionada
    #return indice_programa,anho_seleccionado,indice_estacion,variable_seleccionada,salida_seleccionada,meses_offset



    ## Selecciona los datos correspondientes a la estación y salida seleccionada
    #indice_programa,anho_seleccionado,indice_estacion,indice_salida,cast_seleccionado,meses_offset,variable_seleccionada








###############################################################################
##################### FUNCION PARA INSERTAR DATOS DISCRETOS  ##################
############################################################################### 

def inserta_datos_biogeoquimicos(df_muestreos,df_datos_biogeoquimicos,variables_procesado,variables_procesado_bd,df_referencia):

    # Recupera los datos de conexión
    direccion_host   = st.secrets["postgres"].host
    base_datos       = st.secrets["postgres"].dbname
    usuario          = st.secrets["postgres"].user
    contrasena       = st.secrets["postgres"].password
    puerto           = st.secrets["postgres"].port    

    # Recupera tablas con informacion utilizada en el procesado
    conn                    = init_connection()
    df_indices_calidad      = psql.read_sql('SELECT * FROM indices_calidad', conn)
    df_metodo_ph            = psql.read_sql('SELECT * FROM metodo_pH', conn)
    conn.close()     
   
    # compón un dataframe con la información de muestreo y datos biogeoquímicos
    df_muestreos          = df_muestreos.rename(columns={"id_muestreo": "muestreo"}) # Para igualar los nombres de columnas                                               
    df_datos_disponibles  = pandas.merge(df_datos_biogeoquimicos, df_muestreos, on="muestreo")
    
    # Añade columna con información del año
    df_datos_disponibles['año']                = numpy.zeros(df_datos_disponibles.shape[0],dtype=int)
    for idato in range(df_datos_disponibles.shape[0]):
        df_datos_disponibles['año'].iloc[idato] = (df_datos_disponibles['fecha_muestreo'].iloc[idato]).year
            
    # Despliega menú de selección del programa, año, salida, estación, cast y variable                 
    io_control_calidad = 0
    df_seleccion,indice_estacion,variable_seleccionada,salida_seleccionada,meses_offset = menu_seleccion(df_datos_disponibles,variables_procesado,variables_procesado_bd,io_control_calidad)
    indice_seleccion = variables_procesado_bd.index(variable_seleccionada)
    variable_seleccionada_nombre = variables_procesado[indice_seleccion]

    # Si ya hay datos previos, mostrar un warning        
    if df_seleccion[variable_seleccionada].notnull().all():
        io_valores_prev = 1
        texto_error = "La base de datos ya contiene información para la salida, estación, cast y variable seleccionadas. Los datos introducidos reemplazarán los existentes."
        st.warning(texto_error, icon="⚠️") 
    else:
        io_valores_prev = 0    
        texto_error = "La base de datos NO contiene información para la salida, estación, cast y variable seleccionadas."
        st.warning(texto_error, icon="⚠️") 
        
        
    df_seleccion    = df_seleccion.sort_values('botella')
    
    # Convierte los NaN a None
    #df_seleccion    = df_seleccion.where(pandas.notnull(df_seleccion), None) 
    #df_seleccion = df_seleccion.fillna(None)

    with st.form("Formulario", clear_on_submit=False):

        # Si los datos a introducir son de pH, especificar si la medida es con reactivo purificado o no purificado            
        if variable_seleccionada == 'ph':
       
            listado_metodos   = df_metodo_ph['descripcion_metodo_ph'].tolist()                
            tipo_analisis     = st.radio('Selecciona el tipo de análisis realizado',listado_metodos,horizontal=True,key = 5*df_seleccion.shape[0],index = 0)
            id_tipo_analisis  = df_metodo_ph['id_metodo'][df_metodo_ph['descripcion_metodo_ph']==tipo_analisis].iloc[0] 
            

        
        for idato in range(df_seleccion.shape[0]):
          
            col1, col2,col3,col4 = st.columns(4,gap="small")
            with col1: 
                if df_seleccion['botella'].iloc[idato] is not None and math.isnan(df_seleccion['botella'].iloc[idato]) is False:
                    texto_botella = 'Botella:' + str(int(df_seleccion['botella'].iloc[idato]))
                    st.text(texto_botella)
                
            with col2: 
                
                if df_seleccion['prof_referencia'].iloc[idato] is not None:
                    texto_profunidad = 'Profundidad (m):' + str(int(df_seleccion['prof_referencia'].iloc[idato]))
                
                else:
                    texto_profunidad = 'Presion CTD (db):' + str(round(df_seleccion['presion_ctd'].iloc[idato]))
                st.text(texto_profunidad)

            with col3: 
                texto_variable = variable_seleccionada_nombre + ':'
                if io_valores_prev == 1:
                    valor_entrada  = st.number_input(texto_variable,value=df_seleccion[variable_seleccionada].iloc[idato],key=idato,format = "%f")                                   
                else:
                    valor_entrada  = st.number_input(texto_variable,value=df_referencia[variable_seleccionada][0],key=idato,format = "%f")               
                df_seleccion[variable_seleccionada].iloc[idato] = valor_entrada
                
            with col4: 
                
                variable_seleccionada_cc = variable_seleccionada + '_qf'
                
                if io_valores_prev == 1:
                    indice_calidad_inicial = numpy.where(df_indices_calidad["indice"] ==df_seleccion[variable_seleccionada_cc].iloc[idato])[0][0]
                    listado_indices        = df_indices_calidad['descripcion']
                    qf_seleccionado        = st.selectbox('Índice calidad',(listado_indices),index=int(indice_calidad_inicial),key=(df_seleccion.shape[0] + 1 + idato))                    
                else:
                    qf_seleccionado        = st.selectbox('Índice calidad',(df_indices_calidad['descripcion']),key=(df_seleccion.shape[0] + 1 + idato))
                
                indice_qf_seleccionado = df_indices_calidad['indice'][df_indices_calidad['descripcion']==qf_seleccionado]
                
                
                df_seleccion[variable_seleccionada_cc].iloc[idato] = int(indice_qf_seleccionado)

        io_envio = st.form_submit_button("Asignar valores e índices de calidad definidos")  

        if io_envio:
            
            with st.spinner('Actualizando la base de datos'):
           
                # Introducir los valores en la base de datos
                conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
                cursor = conn.cursor()  
       
                # Diferente instrucción si es pH (hay que especificar el tipo de medida)
                if variable_seleccionada == 'ph': 
                    instruccion_sql = "UPDATE datos_discretos_biogeoquimica SET " + variable_seleccionada + ' = %s, ' + variable_seleccionada +  '_qf = %s, ph_metodo = %s WHERE muestreo = %s;'
                    for idato in range(df_seleccion.shape[0]):
        
                        cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(df_seleccion[variable_seleccionada_cc].iloc[idato]),int(id_tipo_analisis),int(df_seleccion['muestreo'].iloc[idato])))
                        conn.commit()             
                        
                else:
                    instruccion_sql = "UPDATE datos_discretos_biogeoquimica SET " + variable_seleccionada + ' = %s, ' + variable_seleccionada +  '_qf = %s WHERE muestreo = %s;'

                    for idato in range(df_seleccion.shape[0]):
        
                        cursor.execute(instruccion_sql, (df_seleccion[variable_seleccionada].iloc[idato],int(df_seleccion[variable_seleccionada_cc].iloc[idato]),int(df_seleccion['muestreo'].iloc[idato])))
                        conn.commit() 
    
                cursor.close()
                conn.close()   
    
            if io_valores_prev == 1:        
                texto_exito = 'Datos de ' + variable_seleccionada + ' correspondientes a la salida ' + salida_seleccionada + ' actualizados correctamente'
            else:
                texto_exito = 'Datos de ' + variable_seleccionada + ' correspondientes a la salida ' + salida_seleccionada + ' añadidos correctamente'

            st.success(texto_exito)
   



###############################################################################
################ FUNCION PARA COMPROBAR EL ESTADO DEL PROCESADO ###############
############################################################################### 

def comprueba_estado(nombre_programa,fecha_comparacion,nombre_estados,df_estado_procesos):


    estado_procesos_programa = df_estado_procesos[df_estado_procesos['nombre_programa']==nombre_programa]
        
    estado_procesos_programa['Estado']              = None
    estado_procesos_programa['Fecha Actualización'] = None
    estado_procesos_programa['Contacto']            = None
    
    for ianho in range(estado_procesos_programa.shape[0]):

        fecha_final_muestreo       = estado_procesos_programa['fecha_final_muestreo'].iloc[ianho]
        fecha_analisis_laboratorio = estado_procesos_programa['fecha_analisis_laboratorio'].iloc[ianho]
        fecha_post_procesado       = estado_procesos_programa['fecha_post_procesado'].iloc[ianho]
        contacto_muestreo          = estado_procesos_programa['contacto_muestreo'].iloc[ianho]
        contacto_procesado         = estado_procesos_programa['contacto_analisis_laboratorio'].iloc[ianho]
        contacto_post_procesado    = estado_procesos_programa['contacto_post_procesado'].iloc[ianho]
    
        # Comprobacion muestreo 
        if fecha_final_muestreo:
            if fecha_comparacion >= fecha_final_muestreo:
                estado               = nombre_estados[1] 
                contacto             = contacto_muestreo
                fecha_actualizacion  = fecha_final_muestreo
        else:
            estado              = nombre_estados[0]
            contacto            = None
            fecha_actualizacion = None
    
        # Comprobacion procesado 
        if fecha_analisis_laboratorio is not None and fecha_comparacion >= fecha_analisis_laboratorio:
            estado               = nombre_estados[2] 
            contacto             = contacto_procesado
            fecha_actualizacion  = fecha_analisis_laboratorio
    
        # Comprobacion post-procesado 
        if fecha_post_procesado is not None and fecha_comparacion >= fecha_post_procesado:
            estado               = nombre_estados[3] 
            contacto             = contacto_post_procesado
            fecha_actualizacion  = fecha_post_procesado

        estado_procesos_programa['Estado'].iloc[ianho]              = estado
        estado_procesos_programa['Contacto'].iloc[ianho]            = contacto
        if estado_procesos_programa['Fecha Actualización'].iloc[ianho]:
            estado_procesos_programa['Fecha Actualización'].iloc[ianho] = fecha_actualizacion.strftime("%Y-%m-%d")   
         

    # Renombre columnas y elimina las que no se usan
    estado_procesos_programa = estado_procesos_programa.rename(columns={"nombre_programa": "Programa","año": "Año"})
    estado_procesos_programa = estado_procesos_programa.drop(columns=['fecha_final_muestreo','fecha_analisis_laboratorio','fecha_post_procesado','contacto_muestreo','contacto_analisis_laboratorio','contacto_post_procesado','id_proceso','programa'])

    return estado_procesos_programa






###############################################################################
############### FUNCION PARA ACTUALIZAR EL ESTADO DEL PROCESADO ###############
############################################################################### 

def actualiza_estado(datos,id_programa,nombre_programa,fecha_actualizacion,email_contacto,itipo_informacion,direccion_host,base_datos,usuario,contrasena,puerto):

    # Busca de cuántos años diferentes contiene información el dataframe
    vector_auxiliar_tiempo = numpy.zeros(datos.shape[0],dtype=int)
    for idato in range(datos.shape[0]):
        vector_auxiliar_tiempo[idato] = datos['fecha_muestreo'][idato].year
    anhos_muestreados                 = numpy.unique(vector_auxiliar_tiempo)
    datos['año']                      = vector_auxiliar_tiempo 
    
    # Procesado para cada uno de los años incluidos en el dataframe importado
    for ianho in range(len(anhos_muestreados)):
        
        anho_procesado = anhos_muestreados[ianho]
               
        if itipo_informacion == 1:
            # Selecciona la información de cada uno de los años 
            fechas_anuales  = datos['fecha_muestreo'][datos['año']==anhos_muestreados[ianho]]
        
            # Encuentra la fecha de final de muestreo 
            fecha_actualizacion = fechas_anuales.max()

        # Recupera los datos disponibles        
        con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
        conn_psql        = create_engine(con_engine)
        datos_bd         = psql.read_sql('SELECT * FROM estado_procesos', conn_psql)
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
        
    
    
###############################################################################
##################### PÁGINA DE CONSULTA DE DATOS DE BOTELLAS #################
###############################################################################    


def consulta_botellas():
           
    # Función para cargar en caché los datos a utilizar
    @st.cache_data(ttl=600,show_spinner="Cargando información de la base de datos")
    def carga_datos_consulta_botellas():
        conn                    = init_connection()
        df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas            = psql.read_sql('SELECT * FROM programas', conn)
        df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
        df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn)
        df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn)
        df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
        df_variables            = psql.read_sql('SELECT * FROM variables_procesado', conn)
        df_rmn_altos            = psql.read_sql('SELECT * FROM rmn_alto_nutrientes', conn)
        df_rmn_bajos            = psql.read_sql('SELECT * FROM rmn_bajo_nutrientes', conn)
        conn.close() 
        return df_muestreos,df_estaciones,df_datos_biogeoquimicos,df_datos_fisicos,df_salidas,df_programas,df_variables,df_rmn_altos,df_rmn_bajos
           
    st.subheader('Consulta los datos de botellas disponibles') 


    # carga de la caché los datos 
    df_muestreos,df_estaciones,df_datos_biogeoquimicos,df_datos_fisicos,df_salidas,df_programas,df_variables,df_rmn_altos,df_rmn_bajos = carga_datos_consulta_botellas()

    listado_programas       = df_programas['nombre_programa'].tolist()
    id_radiales             = listado_programas.index('RADIAL CORUÑA')
    
    # Despliega menús de selección del programa, tipo de salida, año y fecha               
    col1, col2, col3= st.columns(3,gap="small")
 
    with col1: 
        programa_seleccionado     = st.selectbox('Programa',(df_programas['nombre_programa']),index=id_radiales)   
        id_programa_seleccionado  = int(df_programas['id_programa'][df_programas['nombre_programa']==programa_seleccionado].iloc[0])
        df_salidas_seleccion      = df_salidas[df_salidas['programa']==id_programa_seleccionado]
        
    
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
    
    ### SELECCION DE VARIABLES
  
    listado_variables =['muestreo']  
  
    # Selecciona las variables a exportar
    with st.expander("Variables físicas",expanded=False):
    
        st.write("Selecciona las variables físicas a exportar")    
    
        # Selecciona mostrar o no datos malos y dudosos
        col1, col2, col3, col4 = st.columns(4,gap="small")
        with col1:
            io_temperatura   = st.checkbox('Temperatura(CTD)', value=False)
            if io_temperatura:
                listado_variables = listado_variables + ['temperatura_ctd'] + ['temperatura_ctd_qf']
        with col2:
            io_salinidad     = st.checkbox('Salinidad(CTD)', value=False)
            if io_salinidad:
                listado_variables = listado_variables + ['salinidad_ctd'] + ['salinidad_ctd_qf']
        with col3:
            io_par           = st.checkbox('PAR(CTD)', value=False)
            if io_par:
                listado_variables = listado_variables + ['par_ctd'] + ['par_ctd_qf']
        with col4:
            io_turbidez      = st.checkbox('Turbidez(CTD)', value=False)
            if io_turbidez:
                listado_variables = listado_variables + ['turbidez_ctd'] + ['turbidez_ctd_qf']
                
    # Recorta el dataframe de datos físicos con las variables seleccionadas
    df_datos_fisicos_seleccion = df_datos_fisicos.loc[:, listado_variables]
                    
    listado_variables =['muestreo']
                
    with st.expander("Variables biogeoquímicas",expanded=False):
    
        st.write("Selecciona las variables biogeoquímicas a exportar")    
    
        # Selecciona mostrar o no datos malos y dudosos
        col1, col2, col3, col4 = st.columns(4,gap="small")
        with col1:
            io_fluorescencia   = st.checkbox('Fluorescencia(CTD)', value=False)
            if io_fluorescencia:
                listado_variables = listado_variables + ['fluorescencia_ctd'] + ['fluorescencia_ctd_qf']
 
            io_oxigeno_ctd   = st.checkbox('Oxígeno(CTD)', value=False)
            if io_oxigeno_ctd:
                listado_variables = listado_variables + ['oxigeno_ctd'] + ['oxigeno_ctd_qf']

            io_oxigeno_wk   = st.checkbox('Oxígeno(Winkler)', value=False)
            if io_oxigeno_wk:
                listado_variables = listado_variables + ['oxigeno_wk'] + ['oxigeno_wk_qf']  
                
            io_ph      = st.checkbox('pH', value=False)
            if io_ph:
                listado_variables = listado_variables + ['ph'] + ['ph_qf'] + ['ph_metodo']               

            io_alcalinidad           = st.checkbox('Alcalinidad', value=False)
            if io_alcalinidad:
                 listado_variables = listado_variables + ['alcalinidad'] + ['alcalinidad_qf']  
                             
                
        with col2:
            io_nitrogeno_total     = st.checkbox('TON', value=False)
            if io_nitrogeno_total:
                listado_variables = listado_variables + ['ton'] + ['ton_qf']
                
            io_nitrato   = st.checkbox('Nitrato', value=False)
            if io_nitrato:
                listado_variables = listado_variables + ['nitrato'] + ['nitrato_qf']
  
            io_nitrito   = st.checkbox('Nitrito', value=False)
            if io_nitrito:
                 listado_variables = listado_variables + ['nitrito'] + ['nitrito_qf']

            io_amonio   = st.checkbox('Amonio', value=False)
            if io_amonio:
                 listado_variables = listado_variables + ['amonio'] + ['amonio_qf']                 
                
            io_fosfato   = st.checkbox('Fosfato', value=False)
            if io_fosfato:
                  listado_variables = listado_variables + ['fosfato'] + ['fosfato_qf']                 
   
            io_silicato   = st.checkbox('Silicato', value=False)
            if io_silicato:
                  listado_variables = listado_variables + ['silicato'] + ['silicato_qf'] 
                  
            if io_silicato or io_fosfato or io_amonio or io_nitrito or io_nitrato or io_nitrogeno_total:
                listado_variables = listado_variables + ['cc_nutrientes'] 
                                
                               
        with col3:
            io_tcarb           = st.checkbox('Carbono total', value=False)
            if io_tcarb:
                listado_variables = listado_variables + ['tcarbn'] + ['tcarbn_qf']
                
            io_doc           = st.checkbox('DOC', value=False)
            if io_doc:
                 listado_variables = listado_variables + ['doc'] + ['doc_qf']

            io_cdom           = st.checkbox('CDOM', value=False)
            if io_cdom:
                listado_variables = listado_variables + ['cdom'] + ['cdom_qf']            
                
            io_clorofila_a         = st.checkbox('Clorofila (a)', value=False)
            if io_clorofila_a:
                 listado_variables = listado_variables + ['clorofila_a'] + ['clorofila_a_qf']                 
  
            io_clorofila_b         = st.checkbox('Clorofila (b)', value=False)
            if io_clorofila_b:
                 listado_variables = listado_variables + ['clorofila_b'] + ['clorofila_b_qf']                   
  
            io_clorofila_c         = st.checkbox('Clorofila (c)', value=False)
            if io_clorofila_c:
                 listado_variables = listado_variables + ['clorofila_c'] + ['clorofila_c_qf']    
                 
                 
        with col4:
           io_pp             = st.checkbox('Producción primaria', value=False)
           if io_pp:
               listado_variables = listado_variables + ['prod_primaria'] 
               
           io_doc           = st.checkbox('COP', value=False)
           if io_doc:
                listado_variables = listado_variables + ['cop'] 

           io_cdom           = st.checkbox('NOP', value=False)
           if io_cdom:
               listado_variables = listado_variables + ['nop']           
               
           io_parametros_nutrientes = st.checkbox('Parametros análisis nutrientes', value=False)
           if io_parametros_nutrientes:
               listado_variables = listado_variables + ['rmn_alto_procesado']  + ['rmn_bajo_procesado']  + ['temp_lab_procesado'] + ['rto_columna_procesado']  + ['tubo_nutrientes']  
                
           io_factores_correccion_nutrientes = st.checkbox('Factores de corrección nutrientes', value=False)           
                
    # Recorta el dataframe de datos biogeoquimicos con las variables seleccionadas
    df_datos_biogeoquimicos_seleccion = df_datos_biogeoquimicos.loc[:, listado_variables]
                       
    # EXTRAE DATOS DE LAS VARIABLES Y SALIDAS SELECCIONADAS
    
    if len(listado_salidas) > 0:  
  
        identificadores_salidas         = numpy.zeros(len(listado_salidas),dtype=int)
        for idato in range(len(listado_salidas)):
            identificadores_salidas[idato] = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==listado_salidas[idato]].iloc[0]
            
        # Elimina las columnas que no interesan en los dataframes a utilizar
        #df_salidas_seleccion        = df_salidas_seleccion.drop(df_salidas_seleccion.columns.difference(['id_salida']), 1, inplace=True)
        df_salidas_seleccion        = df_salidas_seleccion.drop(columns=['nombre_salida','programa','nombre_programa','tipo_salida','fecha_salida','hora_salida','fecha_retorno','hora_retorno','buque','estaciones','participantes_comisionados','participantes_no_comisionados','observaciones','año'])

        # conserva los datos de las salidas seleccionadas
        df_salidas_seleccion = df_salidas_seleccion[df_salidas_seleccion['id_salida'].isin(identificadores_salidas)]
  
        # Recupera los muestreos correspondientes a las salidas seleccionadas
        df_muestreos_seleccionados = df_muestreos[df_muestreos['salida_mar'].isin(identificadores_salidas)]
        df_muestreos_seleccionados = df_muestreos_seleccionados.rename(columns={"id_muestreo": "muestreo"})

        # Asocia las coordenadas y nombre de estación de cada muestreo
        df_estaciones               = df_estaciones.rename(columns={"id_estacion": "estacion"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_estaciones, on="estacion")
        
        # Asocia las propiedades físicas de cada muestreo
        #df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_datos_fisicos_seleccion, on="muestreo")
        df_temp = pandas.merge(df_muestreos_seleccionados, df_datos_fisicos_seleccion, on="muestreo")
        if df_temp.shape[0]!=0:
            df_muestreos_seleccionados = df_temp
        
        
        # Asocia las propiedades biogeoquimicas de cada muestreo
        #df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_datos_biogeoquimicos_seleccion, on="muestreo")
        df_temp  = pandas.merge(df_muestreos_seleccionados, df_datos_biogeoquimicos_seleccion, on="muestreo")
        if df_temp.shape[0]!=0:
            df_muestreos_seleccionados = df_temp
            
        # Si se quieren recuperar los parámetros de muestreo de nutrientes, componer el nombre de los rmns utilizados
        if io_parametros_nutrientes:
            df_muestreos_seleccionados = df_muestreos_seleccionados.rename(columns={"rmn_alto_procesado": "rmn_alto_procesado_temp","rmn_bajo_procesado": "rmn_bajo_procesado_temp"})
            df_muestreos_seleccionados['rmn_alto_procesado'] = [None]*df_muestreos_seleccionados.shape[0]
            df_muestreos_seleccionados['rmn_bajo_procesado'] = [None]*df_muestreos_seleccionados.shape[0]
            for idato in range(df_muestreos_seleccionados.shape[0]):
                if df_muestreos_seleccionados['rmn_alto_procesado_temp'].iloc[idato] is not None and df_muestreos_seleccionados['rmn_bajo_procesado_temp'].iloc[idato] is not None:
                    df_muestreos_seleccionados['rmn_alto_procesado'].iloc[idato] = df_rmn_altos['nombre_rmn'][df_rmn_altos['id_rmn']==int(df_muestreos_seleccionados['rmn_alto_procesado_temp'].iloc[idato])]
                    df_muestreos_seleccionados['rmn_bajo_procesado'].iloc[idato] = df_rmn_bajos['nombre_rmn'][df_rmn_bajos['id_rmn']==int(df_muestreos_seleccionados['rmn_bajo_procesado_temp'].iloc[idato])]

    
        # Si se quieren recuperar los factores de corrección de los nutrientes
        if io_factores_correccion_nutrientes:
            df_muestreos_seleccionados = recupera_factores_nutrientes(df_muestreos_seleccionados)
            


            
        # Elimina las columnas que no interesan
        df_exporta                  = df_muestreos_seleccionados.drop(columns=['salida_mar','estacion','programa','prof_referencia','profundidades_referencia','muestreo','latitud_estacion','longitud_estacion'])
    
        #st.dataframe(df_exporta)
    
        # Mueve los identificadores de muestreo al final del dataframe
        listado_cols = df_exporta.columns.tolist()
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('longitud_muestreo')))    
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('latitud_muestreo')))
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('nombre_estacion')))
        listado_cols.insert(0, listado_cols.pop(listado_cols.index('nombre_muestreo')))
        df_exporta = df_exporta[listado_cols]
        
        # Ordena los valores por fechas
        df_exporta = df_exporta.sort_values('fecha_muestreo')
        
        #st.dataframe(df_exporta)

        # Elimina las columnas sin datos        
        nan_value = float("NaN")
        df_exporta.replace("", nan_value, inplace=True)
        df_exporta.dropna(how='all', axis=1, inplace=True)
        
        
        # Añade unidades al nombre de cada variable
        listado_variables_fisicas = df_variables['variables_fisicas'].tolist() 
        listado_unidades_fisicas  = df_variables['unidades_fisica'].tolist() 
        listado_variables_bgq = df_variables['variables_biogeoquimicas'].tolist() 
        listado_unidades_bgq  = df_variables['unidades_bgq'].tolist() 
        
        listado_variables_df = df_exporta.columns.tolist()
        for ivariable_df in range(len(listado_variables_df)):
            if listado_variables_df[ivariable_df] in listado_variables_fisicas:
                indice     = listado_variables_fisicas.index(listado_variables_df[ivariable_df])
                if listado_unidades_fisicas[indice] is not None:
                    nombre_uds = listado_variables_fisicas[indice] + '(' + listado_unidades_fisicas[indice] + ')'
                    df_exporta = df_exporta.rename(columns={listado_variables_df[ivariable_df]: nombre_uds})
            if listado_variables_df[ivariable_df] in listado_variables_bgq:
                indice     = listado_variables_bgq.index(listado_variables_df[ivariable_df])
                if listado_unidades_bgq[indice] is not None:
                    nombre_uds = listado_variables_bgq[indice] + '(' + listado_unidades_bgq[indice] + ')'
                    df_exporta = df_exporta.rename(columns={listado_variables_df[ivariable_df]: nombre_uds})
                
        ## Botón para exportar los resultados
        nombre_archivo =  'DATOS_BOTELLAS.xlsx'
    
        output = BytesIO()
        writer = pandas.ExcelWriter(output, engine='xlsxwriter')
        df_exporta.to_excel(writer, index=False, sheet_name='DATOS')
        # workbook = writer.book
        # worksheet = writer.sheets['DATOS']
        writer.save()
        datos_exporta = output.getvalue()
    
        st.download_button(
            label="DESCARGA LOS DATOS DISPONIBLES DE LOS MUESTREOS SELECCIONADOS",
            data=datos_exporta,
            file_name=nombre_archivo,
            help= 'Descarga un archivo .csv con los datos solicitados',
            mime="application/vnd.ms-excel"
        )

  

        archivo_metadatos     = 'DATOS/Metadatos y control de calidad.pdf'
        with open(archivo_metadatos, "rb") as pdf_file:
            PDFbyte = pdf_file.read()
        
        st.download_button(label="DESCARGA METADATOS",
                            data=PDFbyte,
                            file_name="METADATOS.pdf",
                            help= 'Descarga un archivo .pdf con información de los datos y el control de calidad realizado',
                            mime='application/octet-stream')
    


    
    
    
    
    
###############################################################################
##################### PÁGINA DE CONSULTA DE DATOS DE PERFILES #################
###############################################################################    


def consulta_perfiles():
           
    import matplotlib.pyplot as plt
    import json
 
    @st.cache_data(ttl=600,show_spinner="Cargando información de la base de datos")
    def carga_datos_consulta_perfiles():
        # Recupera tablas con informacion utilizada en el procesado
        conn                    = init_connection()
        df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
        df_programas            = psql.read_sql('SELECT * FROM programas', conn)
        df_perfiles             = psql.read_sql('SELECT * FROM perfiles_verticales', conn)
        df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_perfil_fisica', conn)
        df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_perfil_biogeoquimica', conn)
        df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn)
        conn.close()    
        
        return df_salidas,df_programas,df_perfiles,df_datos_fisicos,df_datos_biogeoquimicos,df_estaciones
 
    
    st.subheader('Consulta los datos de perfiles disponibles') 

    # Carga la información de la base de datos de la caché
    df_salidas,df_programas,df_perfiles,df_datos_fisicos,df_datos_biogeoquimicos,df_estaciones = carga_datos_consulta_perfiles()

    id_radiales             = df_programas['id_programa'][df_programas['nombre_programa']=='RADIAL CORUÑA'].iloc[0]
    df_salidas_seleccion    = df_salidas[df_salidas['programa']==int(id_radiales)]
        
    # Despliega menús de selección del tipo de salida, año y fecha               
    col1, col2= st.columns(2,gap="small")
 
    output = BytesIO()
    writer = pandas.ExcelWriter(output, engine='xlsxwriter')    
  
    with col1:
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
    
    with col2:
        anho_seleccionado           = st.selectbox('Año',(listado_anhos),index=len(listado_anhos)-1)
        df_salidas_seleccion        = df_salidas_seleccion[df_salidas_seleccion['año']==anho_seleccionado]

    # A partir del programa y año elegido, selecciona uno o varios muestreos   
    salida_seleccionada             = st.selectbox('Muestreo',(df_salidas_seleccion['nombre_salida'])) 
    id_salida_seleccionada          = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==salida_seleccionada].iloc[0]
    
    df_perfiles_seleccion  = df_perfiles[df_perfiles['salida_mar']==int(id_salida_seleccionada)]
    df_datos_combinado     = pandas.merge(df_datos_fisicos, df_datos_biogeoquimicos, on="perfil")
        
    # Comprueba si hay datos disponibles
    if df_perfiles_seleccion.shape[0] == 0:
        
        texto_error = "La base de datos no contiene información de perfiles para la salida seleccionada"
        st.warning(texto_error, icon="⚠️")
        
    else:
    
        # Dataframe con los colores con los que representar cada estacion
        data       = [['2', '#b50000'], ['3A', '#70ba07'], ['3B','#0085b5'], ['3C', '#5b00b5'], ['4', '#9d9d9e']]
        df_colores = pandas.DataFrame(data, columns=['estacion', 'color'])
        
        # Listados de variables, unidades y abreviaturas a representar
        listado_variables         = ['temperatura_ctd','salinidad_ctd','par_ctd','fluorescencia_ctd','oxigeno_ctd']
        listado_unidades_grafico  = ['(degC)','(PSU)','(\u03BCE/m2s)','(\u03BCg/L)','(\u03BCmol/kg)']
        listado_unidades_tabla    = ['(degC)','(PSU)','(µE/m2s)','(µg/L)','(µmol/kg)']
        listado_titulos           = ['Temp.','Sal.','PAR','Fluor.','Oxigeno']
        
        listado_variables_adicionales = ['hora_perfil','fecha_perfil','latitud_muestreo','longitud_muestreo','estacion']
        
        import io
        buffer = io.BytesIO()
        
        with pandas.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        
            # Genera un gráfico con tants subplots como variables
            fig, axs = plt.subplots(1, len(listado_variables),figsize=(20/2.54, 18/2.54),sharey='all')
            
            # Bucle por cada perfil disponible
            for iperfil in range(df_perfiles_seleccion.shape[0]):
                
                # Extrae los datos de ese perfil
                df_perfil   = df_datos_combinado[df_datos_combinado['perfil']==df_perfiles_seleccion['perfil'].iloc[iperfil]]
                
                # Asigna el nombre y color de la estación 
                id_estacion     = df_perfiles_seleccion['estacion'].iloc[iperfil]
                nombre_estacion = df_estaciones['nombre_estacion'][df_estaciones['id_estacion']==int(id_estacion)].iloc[0]
                color_estacion  = df_colores['color'][df_colores['estacion']==nombre_estacion].iloc[0]
            
                encabezados_tablas = []
            
                # Representa los datos de cada variable
                for ivariable in range(len(listado_variables)):
                    
                    
                    
                    if df_perfil[listado_variables[ivariable]].iloc[0] is not None:
                        str_datos   = df_perfil[listado_variables[ivariable]].iloc[0]
                        json_datos  = json.loads(str_datos)
                        df_datos    =  pandas.DataFrame.from_dict(json_datos)
                      
                        axs[ivariable].plot(df_datos[listado_variables[ivariable]],df_datos['presion_ctd'],linewidth=1,color=color_estacion,label=nombre_estacion)
                
                    # Almacena los resultados para luego exportar a un excel
                    
                    if ivariable == 0:
                        df_exporta = df_datos
                        # Añade variables adicionales
                        for ivar_adicional in range(len(listado_variables_adicionales)):
                            df_exporta[listado_variables_adicionales[ivar_adicional]] = df_perfiles[listado_variables_adicionales[ivar_adicional]][df_perfiles['perfil']==df_perfiles_seleccion['perfil'].iloc[iperfil]].iloc[0]
                            first_column = df_exporta.pop(listado_variables_adicionales[ivar_adicional])
                            df_exporta.insert(0, listado_variables_adicionales[ivar_adicional], first_column)
                        df_exporta['estacion'] = nombre_estacion
                    else:
                        df_exporta = pandas.concat([df_exporta, df_datos], axis=1)
            
            
                    # Cambia el nombre de las columnas        
                    nombre_encabezado = listado_variables[ivariable] +  listado_unidades_tabla[ivariable]
                    df_exporta        = df_exporta.rename(columns={listado_variables[ivariable]: nombre_encabezado})
            

            
                # Elimina columnas duplicadas (presion_ctd) y exporta a un excel
                df_exporta  = df_exporta.loc[:,~df_exporta.columns.duplicated()].copy()
                nombre_hoja = 'ESTACION '+ nombre_estacion
                df_exporta  = df_exporta.rename(columns={"presion_ctd": "presion_ctd(db)"})
                df_exporta.to_excel(writer, index=False, sheet_name=nombre_hoja)
    
            
            # Ajusta parámetros de los gráficos
            for igrafico in range(len(listado_variables)):
                texto_eje = listado_titulos[igrafico] + listado_unidades_grafico[igrafico] 
                axs[igrafico].set(xlabel=texto_eje)
                axs[igrafico].invert_yaxis()
                if igrafico == 0:
                    axs[igrafico].set(ylabel='Presion (db)')
                    
            # Añade la leyenda
            axs[2].legend(loc='upper center',bbox_to_anchor=(0.5, 1.1),ncol=len(listado_variables), fancybox=True,fontsize=7)
            
            buf = BytesIO()
            fig.savefig(buf, format="png")
            st.image(buf)  
        
        writer.save()
        
        st.download_button(label="DESCARGA LOS DATOS MOSTRADOS",data=buffer,file_name="PERFILES.xlsx",mime="application/vnd.ms-excel")



###############################################################################
################# MENU DE SELECCION DE METADATOS DE LA SALIDA  ################
###############################################################################    


def menu_metadatos_radiales(fecha_salida_defecto,hora_defecto_inicio,fecha_regreso_defecto,hora_defecto_final,df_personal,personal_comisionado_previo,personal_no_comisionado_previo,estaciones_previas,df_estaciones_radiales,id_buque_previo,df_buques,id_perfil_previo,df_config_perfilador,id_sup_previo,df_config_superficie):
           
    df_personal_comisionado    = df_personal[df_personal['comisionado']==True]
    df_personal_no_comisionado = df_personal[df_personal['comisionado']==False]
    fecha_actual               = datetime.date.today()
    
    
    # Bloque de fechas
    col1, col2 = st.columns(2,gap="small")
    
    with col1:               
        fecha_salida  = st.date_input('Fecha de salida',max_value=fecha_actual,value=fecha_salida_defecto)

        hora_salida   = st.time_input('Hora de salida (UTC)', value=hora_defecto_inicio)

    with col2:
        
        fecha_regreso = st.date_input('Fecha de regreso',max_value=fecha_actual,value=fecha_regreso_defecto)

        hora_regreso  = st.time_input('Hora de regreso (UTC)', value=hora_defecto_final)
    

    # Seleccion de buque
    if id_buque_previo is None:
        buque_elegido = st.selectbox('Selecciona el buque utilizado',(df_buques['nombre_buque']))
    else:
        buque_elegido = st.selectbox('Selecciona el buque utilizado',(df_buques['nombre_buque']),index=int(id_buque_previo))            
    id_buque_elegido = int(df_buques['id_buque'][df_buques['nombre_buque']==buque_elegido].values[0])               

    # Asigna la configuracion de los muestreos
    if id_perfil_previo is None:
        id_configurador_perfil = 7 
    else:
        id_configurador_perfil = id_perfil_previo
        
    if id_sup_previo is None:
        id_configurador_sup = 2
    else:
        id_configurador_sup = id_sup_previo

    
    
    # # Bloque de buque y configuracion
    # col1, col2, col3 = st.columns(3,gap="small")    

    # with col1:  
    #     if id_buque_previo is None:
    #         buque_elegido = st.selectbox('Selecciona el buque utilizado',(df_buques['nombre_buque']))
    #     else:
    #         buque_elegido = st.selectbox('Selecciona el buque utilizado',(df_buques['nombre_buque']),index=int(id_buque_previo))            
    #     id_buque_elegido = int(df_buques['id_buque'][df_buques['nombre_buque']==buque_elegido].values[0])               
    
    # with col2:     
    #     if id_perfil_previo is None:
    #         id_configurador_perfil     = st.selectbox('Id.configuracion perfilador',(df_config_perfilador['id_config_perfil']))            
    #     else:
    #         id_configurador_perfil     = st.selectbox('Id.configuracion perfilador',(df_config_perfilador['id_config_perfil']),index=int(id_perfil_previo))

    # with col3:
    #     if id_sup_previo is None:
    #         id_configurador_sup        = st.selectbox('Id.configuracion continuo',(df_config_superficie['id_config_superficie']))
    #     else:
    #         id_configurador_sup        = st.selectbox('Id.configuracion continuo',(df_config_superficie['id_config_superficie']),index=int(id_sup_previo))            

    # Bloque de personal
    
    if personal_comisionado_previo is not None:
        personal_comisionado    = st.multiselect('Personal comisionado participante',df_personal_comisionado['nombre_apellidos'],default=personal_comisionado_previo)
    else:
        personal_comisionado    = st.multiselect('Personal comisionado participante',df_personal_comisionado['nombre_apellidos'])                
    json_comisionados       = json.dumps(personal_comisionado)

    if personal_no_comisionado_previo is not None:            
        personal_no_comisionado = st.multiselect('Personal no comisionado participante',df_personal_no_comisionado['nombre_apellidos'],default=personal_no_comisionado_previo)
    else:
        personal_no_comisionado = st.multiselect('Personal no comisionado participante',df_personal_no_comisionado['nombre_apellidos'])                
    if len(personal_no_comisionado)>0:
        json_no_comisionados    = json.dumps(personal_no_comisionado)
    else:
        json_no_comisionados    = None
    
    if estaciones_previas is not None:
        estaciones_muestreadas  = st.multiselect('Estaciones muestreadas',df_estaciones_radiales['nombre_estacion'],default=estaciones_previas)
    else:                
        estaciones_muestreadas  = st.multiselect('Estaciones muestreadas',df_estaciones_radiales['nombre_estacion'])
    json_estaciones         = json.dumps(estaciones_muestreadas)


    return fecha_salida,hora_salida,fecha_regreso,hora_regreso,json_comisionados,json_no_comisionados,json_estaciones,id_buque_elegido,id_configurador_perfil,id_configurador_sup

    
###############################################################################
###### MENU DE SELECCION DE VARIABLES DE SALIDAS MEDIDAS DURANTE RADIAL #######
###############################################################################    


def menu_variables_radiales(json_variables_previas):
           
    # Selecciona las variables muestreadas
    st.subheader('Variables muestreadas')

    st.markdown('BOTELLAS')

    json_variables = []
    col1, col2, col3, col4= st.columns(4,gap="small")

    with col1:
        if 'Oxigenos' in json_variables_previas:
            oxigenos = st.checkbox('Oxigenos', value=True)
        else:
            oxigenos = st.checkbox('Oxigenos', value=False)                    
        if oxigenos:
            json_variables = json_variables + ['Oxigenos']
            
        if 'Alcalinidad' in json_variables_previas:
            alcalinidad = st.checkbox('Alcalinidad', value=True)
        else:
            alcalinidad = st.checkbox('Alcalinidad', value=False)                    
        if alcalinidad:
            json_variables = json_variables + ['Alcalinidad']

        if 'pH' in json_variables_previas:                    
            ph = st.checkbox('pH', value=True)
        else:
            ph = st.checkbox('pH', value=False)                
        if ph:
            json_variables = json_variables + ['pH']
            
        if 'Nutrientes (A)' in json_variables_previas:                 
            nut_a = st.checkbox('Nutrientes (A)', value=True)
        else:
            nut_a = st.checkbox('Nutrientes (A)', value=False)                    
        if nut_a:
            json_variables = json_variables + ['Nutrientes (A)']
            
        if 'Nutrientes (B)' in json_variables_previas:                 
            nut_b = st.checkbox('Nutrientes (B)', value=True)
        else:
            nut_b = st.checkbox('Nutrientes (B)', value=False)                     
        if nut_b:
            json_variables = json_variables + ['Nutrientes (B)']
           
            
    with col2:                                       
        if 'Citometria' in json_variables_previas:   
            citometria = st.checkbox('Citometria', value=True)
        else:
            citometria = st.checkbox('Citometria', value=False)                    
        if citometria:
            json_variables = json_variables + ['Citometría']

        if 'Ciliados' in json_variables_previas:                                         
            ciliados = st.checkbox('Ciliados', value=True)
        else:
            ciliados = st.checkbox('Ciliados', value=False)                    
        if ciliados:
            json_variables = json_variables + ['Ciliados']

        if 'Zoop. (meso)' in json_variables_previas:                     
            zoop_meso = st.checkbox('Zoop. (meso)', value=True)
        else:
            zoop_meso = st.checkbox('Zoop. (meso)', value=False)                    
        if zoop_meso:
            json_variables = json_variables + ['Zoop. (meso)']

        if 'Zoop. (micro)' in json_variables_previas:                     
            zoop_micro = st.checkbox('Zoop. (micro)', value=True)
        else:
            zoop_micro = st.checkbox('Zoop. (micro)', value=False)                    
        if zoop_micro:
            json_variables = json_variables + ['Zoop. (micro)']   
           
        if 'Zoop. (ictio)' in json_variables_previas: 
            zoop_ictio = st.checkbox('Zoop. (ictio)', value=True)
        else:
            zoop_ictio = st.checkbox('Zoop. (ictio)', value=False)                    
        if zoop_ictio:
            json_variables = json_variables + ['Zoop. (ictio)']   


    with col3:
        if 'Clorofilas' in json_variables_previas: 
            colorofilas = st.checkbox('Clorofilas', value=True)
        else:
            colorofilas = st.checkbox('Clorofilas', value=False)                    
        if colorofilas:
            json_variables = json_variables + ['Clorofilas'] 

        if 'Prod.Primaria' in json_variables_previas: 
            prod_prim = st.checkbox('Prod.Primaria', value=True)
        else:
            prod_prim = st.checkbox('Prod.Primaria', value=False)                    
        if prod_prim:
            json_variables = json_variables + ['Prod.Primaria']
                                
        if 'Flow Cam' in json_variables_previas:                     
            flow_cam = st.checkbox('Flow Cam', value=True)
        else:
            flow_cam = st.checkbox('Flow Cam', value=False)
        if flow_cam:
            json_variables = json_variables + ['Flow Cam'] 
            
        if 'ADN' in json_variables_previas:                    
            adn = st.checkbox('ADN', value=True)
        else:
            adn = st.checkbox('ADN', value=False)                    
        if adn:
            json_variables = json_variables + ['ADN'] 

        if 'DOM' in json_variables_previas:                     
            dom = st.checkbox('DOM', value=True)
        else:
            dom = st.checkbox('DOM', value=False)                
        if dom:
            json_variables = json_variables + ['DOM']
    
    
    with col4:
        if 'TOC' in json_variables_previas:                 
            toc = st.checkbox('TOC', value=True)
        else:
            toc = st.checkbox('TOC', value=False)                    
        if toc:
            json_variables = json_variables + ['TOC']
            
        if 'POC' in json_variables_previas:    
            poc = st.checkbox('POC', value=True)
        else:
            poc = st.checkbox('POC', value=False)                    
        if poc:
            json_variables = json_variables + ['POC']

        if 'PPL' in json_variables_previas:                                    
            ppl = st.checkbox('PPL', value=True)
        else:
            ppl = st.checkbox('PPL', value=False)                    
        if ppl:
            json_variables = json_variables + ['PPL']
            
        otros = st.text_input('Otros:')
        if otros:
            json_variables = json_variables + [otros]

    st.markdown('CONTINUO')
    col1, col2, col3, col4= st.columns(4,gap="small")
    
    with col1:
        if 'Oxigeno (Cont.)' in json_variables_previas:
            oxigenos_continuo = st.checkbox('Oxigeno (Cont.)', value=True)
        else:
            oxigenos_continuo = st.checkbox('Oxigeno (Cont.)', value=False)
        if oxigenos_continuo:
            json_variables = json_variables + ['Oxigeno (Cont.)']  

    with col2:
        if 'pH (Cont.)' in json_variables_previas:
            ph_continuo = st.checkbox('pH (Cont.)', value=True)
        else:
            ph_continuo = st.checkbox('pH (Cont.)', value=False)
        if ph_continuo:
            json_variables = json_variables + ['pH (Cont.)']            

    with col3:
        if 'CDOM (Cont.)' in json_variables_previas:
            cdom_continuo = st.checkbox('CDOM (Cont.)', value=True)
        else:
            cdom_continuo = st.checkbox('CDOM (Cont.)', value=False)                    
        if cdom_continuo:
            json_variables = json_variables + ['CDOM (Cont.)'] 
    
    with col4:
        if 'Clorofila (Cont.)' in json_variables_previas:
            clorofilas_continuo = st.checkbox('Clorofila (Cont.)', value=True)
        else:
            clorofilas_continuo = st.checkbox('Clorofila (Cont.)', value=False)
        if clorofilas_continuo:
            json_variables = json_variables + ['Clorofila (Cont.)']  
            
            
    return json_variables





################################################################
######## FUNCION PARA ASOCIAR A CADA DATO EL FACTOR CORRECTOR DE QC2 DE NUTRIENTES ########
################################################################    

def recupera_factores_nutrientes(df_muestreos_seleccionados):
    

    
    # # Recupera los datos de conexión
    # direccion_host   = st.secrets["postgres"].host
    # base_datos       = st.secrets["postgres"].dbname
    # usuario          = st.secrets["postgres"].user
    # contrasena       = st.secrets["postgres"].password
    # puerto           = st.secrets["postgres"].port

    # Recupera información de la base de datos
    conn                      = init_connection()
    df_factores_nutrientes    = psql.read_sql('SELECT * FROM factores_correctores_nutrientes', conn)
    df_muestreos              = psql.read_sql('SELECT * FROM muestreos_discretos', conn)
    conn.close()


    # Carga la tabla con los factores de corrección
    
    
    
    return df_muestreos_seleccionados