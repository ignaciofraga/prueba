# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 13:09:09 2022

@author: ifraga
"""

import streamlit as st
import psycopg2
import pandas.io.sql as psql
import st_aggrid 
import numpy
from io import BytesIO
import pandas

###############################################################################
###################### FUNCION CONEXIÓN #######################################
###############################################################################

# Funcion para recuperar los parámetros de conexión a partir de los "secrets" establecidos en Streamlit
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
        gb = st_aggrid.grid_options_builder.GridOptionsBuilder.from_dataframe(df_muestreos_curso)
        gridOptions = gb.build()
        st_aggrid.AgGrid(df_muestreos_curso,gridOptions=gridOptions,height = altura_tabla,enable_enterprise_modules=True,allow_unsafe_jscode=True,reload_data=True)    

    else:
        
        texto_error = 'Actualmente no hay ninguna muestra en proceso.'
        st.warning(texto_error, icon="⚠️")         

    return df_muestreos_curso









#################################################################################
######## FUNCION PARA DESPLEGAR MENUS DE SELECCION DE SALIDA Y VARIABLE  ########
#################################################################################
def menu_seleccion(datos_procesados,variables_procesado,variables_procesado_bd,io_control_calidad):

    import streamlit as st
    from FUNCIONES.FUNCIONES_AUXILIARES import init_connection 

    # Recupera los datos disponibles en la base de datos
    conn                      = init_connection()
    df_salidas                = psql.read_sql('SELECT * FROM salidas_muestreos', conn)
    df_estaciones             = psql.read_sql('SELECT * FROM estaciones', conn)
    df_programas              = psql.read_sql('SELECT * FROM programas', conn)
    conn.close()    
    
    # Despliega menús de selección de la variable, salida y la estación a controlar                
    col1, col2 = st.columns(2,gap="small")
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

    col1, col2 = st.columns(2,gap="small")
    with col1: 

        
        listado_salidas           = df_prog_anho_sel['salida_mar'].unique()
        df_salidas_muestreadas    = df_salidas[df_salidas['id_salida'].isin(listado_salidas)]
        df_salidas_muestreadas    = df_salidas_muestreadas.sort_values('fecha_salida',ascending=False)
        salida_seleccionada       = st.selectbox('Salida',(df_salidas_muestreadas['nombre_salida']))
        indice_salida             = df_salidas['id_salida'][df_salidas['nombre_salida']==salida_seleccionada].iloc[0]

        df_prog_anho_sal_sel      = df_prog_anho_sel[df_prog_anho_sel['salida_mar']==indice_salida]

    with col2:

      
        listado_id_estaciones        = df_prog_anho_sal_sel['estacion'].unique() 
        df_estaciones_disponibles    = df_estaciones[df_estaciones['id_estacion'].isin(listado_id_estaciones)]

        estacion_seleccionada        = st.selectbox('Estación',(df_estaciones_disponibles['nombre_estacion']))
        indice_estacion              = df_estaciones_disponibles['id_estacion'][df_estaciones_disponibles['nombre_estacion']==estacion_seleccionada].iloc[0]
        
        df_prog_anho_sal_est_sel     = df_prog_anho_sal_sel[df_prog_anho_sal_sel['estacion']==indice_estacion]
    
    # Un poco diferente según se utilice el menú para control de calidad (con rango de meses) o no (sin él)
    if io_control_calidad == 1:
        col1, col2,col3 = st.columns(3,gap="small")
    else:
        col1, col2      = st.columns(2,gap="small")
    
    with col1: 
        
        listado_casts_estaciones  = df_prog_anho_sal_est_sel['num_cast'].unique() 
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
    
    # Selecciona los datos correspondientes a la estación y salida seleccionada
    df_seleccion               = datos_procesados[(datos_procesados["programa"] == indice_programa) & (datos_procesados["año"] == anho_seleccionado) & (datos_procesados["estacion"] == indice_estacion) & (datos_procesados["salida_mar"] == indice_salida) & (datos_procesados["num_cast"] == cast_seleccionado)]
    
    return df_seleccion,indice_estacion,variable_seleccionada,salida_seleccionada,meses_offset









###############################################################################
##################### PÁGINA DE CONSULTA DE DATOS DE BOTELLAS #################
###############################################################################    


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
  
    ### SELECCION DE VARIABLES
  
    listado_variables =['muestreo']  
  
    # Selecciona las variables a exportar
    with st.expander("Variables físicas",expanded=False):
    
        st.write("Selecciona las variables físicas a exportar")    
    
        # Selecciona mostrar o no datos malos y dudosos
        col1, col2, col3, col4 = st.columns(4,gap="small")
        with col1:
            io_temperatura   = st.checkbox('Temperatura(CTD)', value=True)
            if io_temperatura:
                listado_variables = listado_variables + ['temperatura_ctd'] + ['temperatura_ctd_qf']
        with col2:
            io_salinidad     = st.checkbox('Salinidad(CTD)', value=True)
            if io_salinidad:
                listado_variables = listado_variables + ['salinidad_ctd'] + ['salinidad_ctd_qf']
        with col3:
            io_par           = st.checkbox('PAR(CTD)', value=True)
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
        col1, col2, col3 = st.columns(3,gap="small")
        with col1:
            io_fluorescencia   = st.checkbox('Fluorescencia(CTD)', value=True)
            if io_fluorescencia:
                listado_variables = listado_variables + ['fluorescencia_ctd'] + ['fluorescencia_ctd_qf']
 
            io_oxigeno_ctd   = st.checkbox('Oxígeno(CTD)', value=True)
            if io_oxigeno_ctd:
                listado_variables = listado_variables + ['oxigeno_ctd'] + ['oxigeno_ctd_qf']

            io_oxigeno_wk   = st.checkbox('Oxígeno(Winkler)', value=True)
            if io_oxigeno_wk:
                listado_variables = listado_variables + ['oxigeno_wk'] + ['oxigeno_wk_qf']  
                
            io_ph      = st.checkbox('pH', value=True)
            if io_ph:
                listado_variables = listado_variables + ['ph'] + ['ph_qf'] + ['ph_metodo']               

            io_alcalinidad           = st.checkbox('Alcalinidad', value=True)
            if io_alcalinidad:
                 listado_variables = listado_variables + ['alcalinidad'] + ['alcalinidad_qf']  
                             
                
        with col2:
            io_nitrogeno_total     = st.checkbox('Nitrógeno total', value=True)
            if io_nitrogeno_total:
                listado_variables = listado_variables + ['nitrogeno_total'] + ['nitrogeno_total_qf']
                
            io_nitrato   = st.checkbox('Nitrato', value=True)
            if io_nitrato:
                listado_variables = listado_variables + ['nitrato'] + ['nitrato_qf']
  
            io_nitrito   = st.checkbox('Nitrito', value=True)
            if io_nitrito:
                 listado_variables = listado_variables + ['nitrito'] + ['nitrito_qf']

            io_amonio   = st.checkbox('Amonio', value=True)
            if io_amonio:
                 listado_variables = listado_variables + ['amonio'] + ['amonio_qf']                 
                
            io_fosfato   = st.checkbox('Fosfato', value=True)
            if io_fosfato:
                  listado_variables = listado_variables + ['fosfato'] + ['fosfato_qf']                 
   
            io_silicato   = st.checkbox('Silicato', value=True)
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
                
            io_clorofila           = st.checkbox('Clorofila (a)', value=False)
            if io_clorofila:
                 listado_variables = listado_variables + ['clorofila_a'] + ['clorofila_a_qf']                 
                
                
    # Recorta el dataframe de datos biogeoquimicos con las variables seleccionadas
    df_datos_biogeoquimicos_seleccion = df_datos_biogeoquimicos.loc[:, listado_variables]
                
    # Si se exportan datos de pH, corregir la informacion del método utilizado
    if io_ph:
        conn                    = init_connection()
        df_metodos_ph           = psql.read_sql('SELECT * FROM metodo_pH', conn)
        conn.close()     
               
        df_metodos_ph                      = df_metodos_ph.rename(columns={"id_metodo": "ph_metodo"})
        df_datos_biogeoquimicos_seleccion  = pandas.merge(df_datos_biogeoquimicos_seleccion, df_metodos_ph, on="ph_metodo")
        df_datos_biogeoquimicos_seleccion  = df_datos_biogeoquimicos_seleccion.drop(columns=['ph_metodo','metodo_ph'])
        df_datos_biogeoquimicos_seleccion  = df_datos_biogeoquimicos_seleccion.rename(columns={"descripcion_metodo_ph": "metodo_pH"})        
        
        listado_columnas = df_datos_biogeoquimicos_seleccion.columns.tolist()
        listado_columnas.insert(listado_columnas.index('ph_qf')+1,listado_columnas.pop(listado_columnas.index('metodo_pH')))
        df_datos_biogeoquimicos_seleccion = df_datos_biogeoquimicos_seleccion[listado_columnas]
    
    
    # EXTRAE DATOS DE LAS VARIABLES Y SALIDAS SELECCIONADAS
     
    if len(listado_salidas) > 0:  
  
        identificadores_salidas         = numpy.zeros(len(listado_salidas),dtype=int)
        for idato in range(len(listado_salidas)):
            identificadores_salidas[idato] = df_salidas_seleccion['id_salida'][df_salidas_seleccion['nombre_salida']==listado_salidas[idato]].iloc[0]
    
        # Elimina las columnas que no interesan en los dataframes a utilizar
        #df_salidas_seleccion        = df_salidas_seleccion.drop(df_salidas_seleccion.columns.difference(['id_salida']), 1, inplace=True)
        df_salidas_seleccion        = df_salidas_seleccion.drop(columns=['nombre_salida','programa','nombre_programa','tipo_salida','fecha_salida','hora_salida','fecha_retorno','hora_retorno','buque','estaciones','participantes_comisionados','participantes_no_comisionados','observaciones','año'])
        df_muestreos                = df_muestreos.drop(columns=['configuracion_perfilador','configuracion_superficie'])

        # conserva los datos de las salidas seleccionadas
        df_salidas_seleccion = df_salidas_seleccion[df_salidas_seleccion['id_salida'].isin(identificadores_salidas)]
    
        # Recupera los muestreos correspondientes a las salidas seleccionadas
        df_muestreos                = df_muestreos.rename(columns={"salida_mar": "id_salida"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_salidas_seleccion, df_muestreos, on="id_salida")
                              
        # Asocia las coordenadas y nombre de estación de cada muestreo
        df_estaciones               = df_estaciones.rename(columns={"id_estacion": "estacion"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_estaciones, on="estacion")
        
        # Asocia las propiedades físicas de cada muestreo
        df_datos_fisicos            = df_datos_fisicos_seleccion.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_datos_fisicos, on="id_muestreo")
    
        # Asocia las propiedades biogeoquimicas de cada muestreo
        df_datos_biogeoquimicos     = df_datos_biogeoquimicos_seleccion.rename(columns={"muestreo": "id_muestreo"}) # Para igualar los nombres de columnas                                               
        df_muestreos_seleccionados  = pandas.merge(df_muestreos_seleccionados, df_datos_biogeoquimicos, on="id_muestreo")
    
        # Elimina las columnas que no interesan
        df_exporta                  = df_muestreos_seleccionados.drop(columns=['id_salida','estacion','programa','prof_referencia','profundidades_referencia','id_muestreo','variables_muestreadas'])
    
        # Mueve os identificadores de muestreo al final del dataframe
        listado_cols = df_exporta.columns.tolist()
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

  

        archivo_metadatos     = 'DATOS/Metadatos y control de calidad.pdf'
        with open(archivo_metadatos, "rb") as pdf_file:
            PDFbyte = pdf_file.read()
        
        st.download_button(label="DESCARGA METADATOS",
                            data=PDFbyte,
                            file_name="METADATOS.pdf",
                            help= 'Descarga un archivo .pdf con información de los datos y el control de calidad realizado',
                            mime='application/octet-stream')
    





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
        
        
    df_seleccion    = df_seleccion.sort_values('botella')

    with st.form("Formulario", clear_on_submit=False):

        # Si los datos a introducir son de pH, especificar si la medida es con reactivo purificado o no purificado            
        if variable_seleccionada == 'ph':
       
            listado_metodos   = df_metodo_ph['descripcion_metodo_ph'].tolist()                
            tipo_analisis     = st.radio('Selecciona el tipo de análisis realizado',listado_metodos,horizontal=True,key = 5*df_seleccion.shape[0],index = 0)
            id_tipo_analisis  = df_metodo_ph['id_metodo'][df_metodo_ph['descripcion_metodo_ph']==tipo_analisis].iloc[0] 
            

        for idato in range(df_seleccion.shape[0]):
          
            col1, col2,col3,col4 = st.columns(4,gap="small")
            with col1: 
                
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
##################### FUNCION PARA INSERTAR DATOS DISCRETOS  ##################
############################################################################### 

def comprueba_estado(id_programa,fecha_comparacion):

#     # Consulta a la base de datos las fechas de cada proceso
#     conn = init_connection()
#     cursor = conn.cursor()           
# #    instruccion_sql = 'SELECT fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_analisis_laboratorio,contacto_post_procesado FROM estado_procesos WHERE programa = ' + str(id_programa) +' AND año = ' + str(anho_proceso) +';'
#     instruccion_sql = 'SELECT fecha_final_muestreo,fecha_analisis_laboratorio,fecha_post_procesado,contacto_muestreo,contacto_analisis_laboratorio,contacto_post_procesado FROM estado_procesos WHERE programa = %s AND año = %s;'
#     cursor.execute(instruccion_sql,(int(id_programa),int(anho_proceso))) 
#     datos_bd =cursor.fetchall()         
#     cursor.close()
#     conn.close()      


    # Recupera la tabla del estado de los procesos como un dataframe
    conn = init_connection()
    estado_procesos = psql.read_sql('SELECT * FROM estado_procesos', conn)
    conn.close()

    estado_procesos_programa = estado_procesos[estado_procesos['programa']==id_programa]

    df_estados = pandas.DataFrame(columns=['Programa','Año','Estado','Fecha Actualización','Contacto'])
                
    for ianho in range(estado_procesos_programa.shape[0]):

        
        fecha_final_muestreo       = estado_procesos_programa['fecha_final_muestreo'].iloc[ianho]
        fecha_analisis_laboratorio = estado_procesos_programa['fecha_analisis_laboratorio'].iloc[ianho]
        fecha_post_procesado       = estado_procesos_programa['fecha_post_procesado'].iloc[ianho]
        contacto_muestreo          = estado_procesos_programa['contacto_muestreo'].iloc[ianho]
        contacto_procesado         = estado_procesos_programa['contacto_analisis_laboratorio'].iloc[ianho]
        contacto_post_procesado    = estado_procesos_programa['contacto_post_procesado'].iloc[ianho]

        st.text(ianho)
        st.text(fecha_analisis_laboratorio)
    
        # Comprobacion muestreo 
        if fecha_final_muestreo:
            if fecha_comparacion >= fecha_final_muestreo:
                iestado              = 1 
                contacto             = contacto_muestreo
                fecha_actualizacion  = fecha_final_muestreo
        else:
            iestado             = 0
            contacto            = None
            fecha_actualizacion = None
    
        # Comprobacion procesado 
        if fecha_analisis_laboratorio is not None and fecha_comparacion >= fecha_analisis_laboratorio:
            iestado              = 2 
            contacto             = contacto_procesado
            fecha_actualizacion  = fecha_analisis_laboratorio
    
        # Comprobacion post-procesado 
        if fecha_post_procesado is not None and fecha_comparacion >= fecha_post_procesado:
            iestado              = 3 
            contacto             = contacto_post_procesado
            fecha_actualizacion  = fecha_post_procesado

        df_estados['Estado'].loc[ianho]              = iestado
        df_estados['Fecha Actualización'].loc[ianho] = fecha_actualizacion    
        df_estados['Contacto'].loc[ianho]            = contacto   

    return df_estados


    # # Caso 3. Fecha de consulta posterior al post-procesado.
    # if fecha_post_procesado:
    #     if tiempo_consulta >= (estado_procesos_programa['fecha_post_procesado'][ianho]):     
    #         estado_procesos_programa['id_estado'][ianho] = 3
    #         estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho] 
    #         estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_post_procesado'][ianho].strftime("%m/%d/%Y")
    # else:
        
    #     # Caso 2. Fecha de consulta posterior al análisis de laboratorio pero anterior a realizar el post-procesado.
    #     if pandas.isnull(estado_procesos_programa['fecha_analisis_laboratorio'][ianho]) is False:
    #         if tiempo_consulta >= (estado_procesos_programa['fecha_analisis_laboratorio'][ianho]):  # estado_procesos_programa['fecha_analisis_laboratorio'][ianho] is not None:     
    #             estado_procesos_programa['id_estado'][ianho] = 2
    #             estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho] 
    #             estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_analisis_laboratorio'][ianho].strftime("%m/%d/%Y")
    #         else:
    #             if tiempo_consulta >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
    #                 estado_procesos_programa['id_estado'][ianho] = 1 
    #                 estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho]
    #                 estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_entrada_datos'][ianho].strftime("%m/%d/%Y")
                                        
        
    #     else:
    #         # Caso 1. Fecha de consulta posterior a terminar la campaña pero anterior al análisis en laboratorio, o análisis no disponible. 
    #         if pandas.isnull(estado_procesos_programa['fecha_entrada_datos'][ianho]) is False:
    #             if tiempo_consulta >= (estado_procesos_programa['fecha_entrada_datos'][ianho]): #estado_procesos_programa['fecha_final_muestreo'][ianho] is not None:
    #                 estado_procesos_programa['id_estado'][ianho] = 1 
    #                 estado_procesos_programa['contacto'][ianho] = estado_procesos_programa['contacto_post_procesado'][ianho]
    #                 estado_procesos_programa['fecha actualizacion'][ianho] = estado_procesos_programa['fecha_entrada_datos'][ianho].strftime("%m/%d/%Y")
                