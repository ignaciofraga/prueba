# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""








import FUNCIONES_PROCESADO
import pandas
pandas.options.mode.chained_assignment = None
import numpy
from sqlalchemy import create_engine
import pandas.io.sql as psql


# # Parámetros
# base_datos     = 'COAC'
# usuario        = 'postgres'
# contrasena     = 'm0nt34lt0'
# puerto         = '5432'
# direccion_host = '193.146.155.99'

# programa_muestreo = 'RADIAL CORUÑA'

# nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/HISTORICO/HISTORICO_FINAL.xlsx'
    
def inserta_radiales_historico(nombre_archivo,base_datos,usuario,contrasena,puerto,direccion_host,programa_muestreo):

    # Importa el .xlsx
    datos_radiales = pandas.read_excel(nombre_archivo, 'datos',na_values='#N/A')
    
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
            datos_radiales['densidad'].iloc[idato]  =  1 + datos_radiales['sigmat'].iloc[idato]/1000
    
    datos_radiales = datos_radiales.rename(columns={"Fecha": "fecha_muestreo", "Prof":"presion_ctd", "t":"temperatura_ctd","S":"salinidad_ctd","E":"par_ctd", 
                                                    "O2 umol/kg":"oxigeno_wk","Cla":"clorofila_a","ID_estacion":"estacion","Clb":"clorofila_b","Clc":"clorofila_c","PP":"prod_primaria","COP":"cop",'NOP':'nop'})
    
    
    # correccion
    datos_radiales['ton']      = [None]*datos_radiales.shape[0]
    datos_radiales['nitrato']  = [None]*datos_radiales.shape[0]
    datos_radiales['nitrito']  = [None]*datos_radiales.shape[0]
    datos_radiales['amonio']   = [None]*datos_radiales.shape[0]
    datos_radiales['fosfato']  = [None]*datos_radiales.shape[0]
    datos_radiales['silicato'] = [None]*datos_radiales.shape[0]
    
    
    datos_radiales['ton_qf']      = numpy.ones(datos_radiales.shape[0])
    datos_radiales['nitrato_qf']  = numpy.ones(datos_radiales.shape[0])
    datos_radiales['nitrito_qf']  = numpy.ones(datos_radiales.shape[0])
    datos_radiales['amonio_qf']   = numpy.ones(datos_radiales.shape[0])
    datos_radiales['fosfato_qf']  = numpy.ones(datos_radiales.shape[0])
    datos_radiales['silicato_qf'] = numpy.ones(datos_radiales.shape[0])
    
    datos_radiales['botella']       = [None]*datos_radiales.shape[0]
    datos_radiales['hora_muestreo'] = [None]*datos_radiales.shape[0]
    datos_radiales['prof_referencia'] = [None]*datos_radiales.shape[0]
    datos_radiales['num_cast']        = [None]*datos_radiales.shape[0]
    
    datos_radiales['salinidad_ctd_qf']   = int(2)
    datos_radiales['temperatura_ctd_qf'] = int(2)
    datos_radiales['par_ctd_qf']         = int(2)
    
    datos_radiales['oxigeno_wk_qf']      = int(2)
    datos_radiales['clorofila_a_qf']     = int(2)
    datos_radiales['clorofila_b_qf']     = int(2)
    datos_radiales['clorofila_c_qf']     = int(2)
    
    for idato in range(datos_radiales.shape[0]):
        if datos_radiales['NO3'].iloc[idato] is not None:
            datos_radiales['nitrato'].iloc[idato]  = datos_radiales['NO3'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['nitrato_qf'].iloc[idato] = 9
              
        if datos_radiales['NO2'].iloc[idato] is not None:
            datos_radiales['nitrito'].iloc[idato]  = datos_radiales['NO2'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['nitrito_qf'].iloc[idato]  = 9       
        
        if datos_radiales['NH4'].iloc[idato] is not None:
            datos_radiales['amonio'].iloc[idato]  = datos_radiales['NH4'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['amonio_qf'].iloc[idato]  = 9
        
        if datos_radiales['PO4'].iloc[idato] is not None:
            datos_radiales['fosfato'].iloc[idato]  = datos_radiales['PO4'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['fosfato_qf'].iloc[idato]  = 9
            
        if datos_radiales['SiO2'].iloc[idato] is not None:
            datos_radiales['silicato'].iloc[idato]  = datos_radiales['SiO2'].iloc[idato]/datos_radiales['densidad'].iloc[idato]  
        else:
            datos_radiales['silicato_qf'].iloc[idato]  = 9  
            
            
        # QF
        if datos_radiales['NO3flag'].iloc[idato] is not None:
            datos_radiales['nitrato_qf'].iloc[idato]  = datos_radiales['NO3flag'].iloc[idato]  
        if datos_radiales['NO2flag'].iloc[idato] is not None:
            datos_radiales['nitrito_qf'].iloc[idato]  = datos_radiales['NO2flag'].iloc[idato]  
        if datos_radiales['NH4flag'].iloc[idato] is not None:
            datos_radiales['amonio_qf'].iloc[idato]  = datos_radiales['NH4flag'].iloc[idato]
        if datos_radiales['PO4flag'].iloc[idato] is not None:
            datos_radiales['fosfato_qf'].iloc[idato]  = datos_radiales['PO4flag'].iloc[idato]          
        if datos_radiales['SiO2flag'].iloc[idato] is not None:
            datos_radiales['silicato_qf'].iloc[idato]  = datos_radiales['SiO2flag'].iloc[idato]           
    
        # calculo del TON 
        if datos_radiales['nitrato'].iloc[idato] is not None and datos_radiales['nitrito'].iloc[idato] is not None:
            datos_radiales['ton'].iloc[idato]  = datos_radiales['nitrato'].iloc[idato] + datos_radiales['nitrito'].iloc[idato]
            if datos_radiales['amonio'].iloc[idato] is not None:
                datos_radiales['ton'].iloc[idato] = datos_radiales['ton'].iloc[idato] + datos_radiales['amonio'].iloc[idato]
        else:
            datos_radiales['ton_qf'].iloc[idato] = 9
        
        # Reviso los QF
        if datos_radiales['salinidad_ctd'].iloc[idato] is None:
            datos_radiales['salinidad_ctd_qf'].iloc[idato] = 9
        if datos_radiales['temperatura_ctd'].iloc[idato] is None:
            datos_radiales['temperatura_ctd_qf'].iloc[idato] = 9
        if datos_radiales['par_ctd'].iloc[idato] is None:
            datos_radiales['par_ctd_qf'].iloc[idato] = 9
        if datos_radiales['oxigeno_wk'].iloc[idato] is None:
            datos_radiales['oxigeno_wk_qf'].iloc[idato] = 9
        if datos_radiales['clorofila_a'].iloc[idato] is None:
            datos_radiales['clorofila_a_qf'].iloc[idato] = 9
        if datos_radiales['clorofila_b'].iloc[idato] is None:
            datos_radiales['clorofila_b_qf'].iloc[idato] = 9
        if datos_radiales['clorofila_c'].iloc[idato] is None:
            datos_radiales['clorofila_c_qf'].iloc[idato] = 9         
            
        # aprovecho para cambiar el nombre de la estación
        if datos_radiales['estacion'].iloc[idato] == 'E2CO':
            datos_radiales['estacion'].iloc[idato]  = '2'    
        if datos_radiales['estacion'].iloc[idato] == 'E4CO':
            datos_radiales['estacion'].iloc[idato]  = '4'    
        if datos_radiales['estacion'].iloc[idato] == 'E3CO':
            datos_radiales['estacion'].iloc[idato]  = '3' 
        if datos_radiales['estacion'].iloc[idato] == 'E3ACO':
            datos_radiales['estacion'].iloc[idato]  = '3A' 
        if datos_radiales['estacion'].iloc[idato] == 'E3BCO':
            datos_radiales['estacion'].iloc[idato]  = '3B' 
        if datos_radiales['estacion'].iloc[idato] == 'E3CCO':
            datos_radiales['estacion'].iloc[idato]  = '3C'         
    
    
    
    
    # Recupera el identificador del programa de muestreo
    id_programa,abreviatura_programa = FUNCIONES_PROCESADO.recupera_id_programa(programa_muestreo,direccion_host,base_datos,usuario,contrasena,puerto)
    
    # Encuentra la estación asociada a cada registro
    print('Asignando la estación correspondiente a cada medida')
    datos_radiales = FUNCIONES_PROCESADO.evalua_estaciones(datos_radiales,id_programa,direccion_host,base_datos,usuario,contrasena,puerto)
    
    
    # Encuentra las salidas al mar correspondientes
    print('Asignando la salida correspondiente a cada medida')
    tipo_salida    = 'MENSUAL'   
    datos_radiales = FUNCIONES_PROCESADO.evalua_salidas(datos_radiales,id_programa,programa_muestreo,tipo_salida,direccion_host,base_datos,usuario,contrasena,puerto)
     
    # Encuentra el identificador asociado a cada registro
    print('Asignando el registro correspondiente a cada medida')
    datos_radiales = FUNCIONES_PROCESADO.evalua_registros(datos_radiales,abreviatura_programa,direccion_host,base_datos,usuario,contrasena,puerto)
       
    # # # # # Introduce los datos en la base de datos
    print('Introduciendo los datos en la base de datos')
    FUNCIONES_PROCESADO.inserta_datos(datos_radiales,'discreto_fisica',direccion_host,base_datos,usuario,contrasena,puerto)
    FUNCIONES_PROCESADO.inserta_datos(datos_radiales,'discreto_bgq',direccion_host,base_datos,usuario,contrasena,puerto)




def recupera_id(fecha_umbral,usuario,contrasena,direccion_host,puerto,base_datos):
   
    #fecha_umbral = datetime.date(2018,1,1)

    # Recupera la tabla con los registros de los muestreos
    con_engine       = 'postgresql://' + usuario + ':' + contrasena + '@' + direccion_host + ':' + str(puerto) + '/' + base_datos
    conn_psql        = create_engine(con_engine)
    df_muestreos     = psql.read_sql('SELECT * FROM muestreos_discretos', conn_psql)
    conn_psql.dispose()   

    nombre_archivo    = 'C:/Users/ifraga/Desktop/03-DESARROLLOS/BASE_DATOS_COAC/DATOS/RADIALES/HISTORICO/HISTORICO_FINAL.xlsx' 
    datos_radiales    = pandas.read_excel(nombre_archivo, 'datos',na_values='#N/A')

    # Convierte las fechas de DATE a formato correcto
    datos_radiales['Fecha'] =  pandas.to_datetime(datos_radiales['Fecha'], format='%Y%m%d').dt.date

    datos = datos_radiales[datos_radiales['Fecha']>fecha_umbral]

    datos['estacion'] = [None]*datos.shape[0]
    datos['prof_bd'] = [None]*datos.shape[0]
    datos['nombre_muestreo'] = [None]*datos.shape[0]

    for idato in range(datos.shape[0]):
      
        if datos['ID_estacion'].iloc[idato] == 'E2CO':
            datos['estacion'].iloc[idato] = 1
        if datos['ID_estacion'].iloc[idato] == 'E4CO':       
            datos['estacion'].iloc[idato] = 5
            
        if datos['estacion'].iloc[idato]  == 5 or datos['estacion'].iloc[idato]  == 1:
                    
            df_temp = df_muestreos[(df_muestreos['fecha_muestreo']==datos['Fecha'].iloc[idato]) & (df_muestreos['estacion']==datos['estacion'].iloc[idato])]


            dif_profs     = numpy.asarray(abs(df_temp['presion_ctd'] - datos['Prof'].iloc[idato]))
            indice_posicion = numpy.argmin(dif_profs)

            datos['prof_bd'].iloc[idato] = df_temp['presion_ctd'].iloc[indice_posicion]
            datos['nombre_muestreo'].iloc[idato] = df_temp['nombre_muestreo'].iloc[indice_posicion]

    datos = datos[datos['nombre_muestreo'].notna()]
    datos = datos.rename(columns={"Cla": "clorofila_a", "Clb": "clorofila_b","Clc":"clorofila_c","PP":"prod_primaria","COP":"cop", "NOP": "nop"})        					
    datos_recorte = datos[['nombre_muestreo','clorofila_a','clorofila_b','clorofila_c','prod_primaria','cop','nop']]

    return datos_recorte