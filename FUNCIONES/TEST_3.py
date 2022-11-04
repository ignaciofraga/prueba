# -*- coding: utf-8 -*-
"""
Created on Thu Nov  3 14:28:35 2022

@author: ifraga
"""


base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

import pandas
pandas.options.mode.chained_assignment = None
import datetime

import os
import numpy
import pandas.io.sql as psql
from sqlalchemy import create_engine
import psycopg2
import os
from glob import glob
import json
import matplotlib.pyplot as plt
con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos


conn_psql        = create_engine(con_engine)


# Recupera tablas con informacion utilizada en el procesado

df_muestreos            = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
df_datos_fisicos        = psql.read_sql('SELECT * FROM datos_discretos_fisica', conn_psql)
df_datos_biogeoquimicos = psql.read_sql('SELECT * FROM datos_discretos_biogeoquimica', conn_psql)
df_salidas              = psql.read_sql('SELECT * FROM salidas_muestreos', conn_psql)
df_programas            = psql.read_sql('SELECT * FROM programas', conn_psql)
df_estaciones           = psql.read_sql('SELECT * FROM estaciones', conn_psql)
df_indices_calidad      = psql.read_sql('SELECT * FROM indices_calidad', conn_psql)
conn_psql.dispose()  

id_salida = 1


    
# Recupera los muestreos de la salida seleccionada
df_muestreos_salida = df_muestreos[df_muestreos['salida_mar']==id_salida]  

if df_muestreos_salida.shape[0] == 0:
    
    texto_error = 'No hay datos disponibles para la salida seleccionada '
#    st.warning(texto_error, icon="⚠️")        
    
else:

    # Determina las estaciones muestreadas en la salida selecionada
    listado_estaciones         = df_muestreos_salida['estacion'].unique()
    df_estaciones_muestreadas  = df_estaciones[df_estaciones['id_estacion'].isin(listado_estaciones)]
    nombres_estaciones         = df_estaciones_muestreadas['nombre_estacion'].tolist()
    listado_estaciones         = df_estaciones_muestreadas['id_estacion'].tolist()
    
    # # Despliega menús de selección de la variable y la estación a controlar                
    # col1, col2 = st.columns(2,gap="small")
 
    # with col1: 
    estacion_seleccionada = 'E2CO'
    indice_estacion       = listado_estaciones[nombres_estaciones.index(estacion_seleccionada)]
    df_muestreos_estacion = df_muestreos_salida[df_muestreos_salida['estacion']==indice_estacion]
    listado_muestreos     = df_muestreos_estacion['id_muestreo']
    
    #with col2:
    listado_variables     = ['temperatura_ctd','salinidad_ctd','par_ctd','fluorescencia_ctd','oxigeno_ctd']
    nombre_variables      = ['Temperatura','Salinidad','PAR','Fluorescencia','O2']
    uds_variables         = ['ºC','psu','\u03BCE/m2.s1','\u03BCg/kg','\u03BCmol/kg']
    variable_seleccionada = 'O2'
    
    indice_variable = nombre_variables.index(variable_seleccionada)

    if indice_variable <=2: # Datos fisicos
        df_temp         = df_datos_fisicos[df_datos_fisicos['muestreo'].isin(listado_muestreos)]
        tabla_actualiza = 'datos_discretos_fisica'
        identificador   = 'id_disc_fisica'
    else:                    # Datos biogeoquimicos
        df_temp         = df_datos_biogeoquimicos[df_datos_biogeoquimicos['muestreo'].isin(listado_muestreos)]        
        tabla_actualiza = 'datos_discretos_biogeoquimica'
        identificador   = 'id_disc_biogeoquim'
        
    datos_variable    = df_temp[listado_variables[indice_variable]]
      

    # Representa un gráfico con la variable seleccionada
    fig, ax = plt.subplots()
    ax.plot(datos_variable,df_muestreos_estacion['presion_ctd'],'.k' )
    texto_eje = nombre_variables[indice_variable] + '(' + uds_variables[indice_variable] + ')'
    ax.set(xlabel=texto_eje)
    ax.set(ylabel='Presion (db)')
    ax.invert_yaxis()
    # Añade el nombre de cada punto
    nombre_muestreos = [None]*len(datos_variable)
    for ipunto in range(len(datos_variable)):
        if df_muestreos_estacion['botella'].iloc[ipunto] is None:
            nombre_muestreos[ipunto] = 'Prof.' + str(df_muestreos_estacion['presion_ctd'].iloc[ipunto])
        else:
            nombre_muestreos[ipunto] = 'Bot.' + str(df_muestreos_estacion['botella'].iloc[ipunto])
        ax.annotate(nombre_muestreos[ipunto], (datos_variable.iloc[ipunto], df_muestreos_estacion['presion_ctd'].iloc[ipunto]))
    
    # st.pyplot(fig)

    # #
    # with st.form("Formulario", clear_on_submit=True):
                   
    #     indice_validacion = df_indices_calidad['indice'].tolist()
    #     texto_indice      = df_indices_calidad['descripcion'].tolist()
    #     qf_asignado       = numpy.zeros(len(datos_variable))
        
    #     for idato in range(len(datos_variable)):
            
    #         enunciado          = 'QF del muestreo ' + nombre_muestreos[idato]
    #         valor_asignado     = st.radio(enunciado,texto_indice,horizontal=True,key = idato,index = 1)
    #         qf_asignado[idato] = indice_validacion[texto_indice.index(valor_asignado)]
        
    #     io_envio = st.form_submit_button("Asignar los índices seleccionados")  
 
    # if io_envio:
        
    #     texto_estado = 'Actualizando los índices de la base de datos'
    #     with st.spinner(texto_estado):
        
    #         # Introducir los valores en la base de datos
    #         conn   = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
    #         cursor = conn.cursor()  
    
    #         for idato in range(len(datos_variable)):
 
    #             instruccion_sql = "UPDATE " + tabla_actualiza + " SET " + listado_variables[indice_variable] + '_qf = %s WHERE ' + identificador + '= %s;'
    #             cursor.execute(instruccion_sql, (int(qf_asignado[idato],int(df_temp[identificador].iloc[idato]))))
    #             conn.commit() 

    #         cursor.close()
    #         conn.close()   
 
    #     texto_exito = 'QF de la variable  ' + variable_seleccionada + ' asignados correctamente'
    #     st.success(texto_exito)