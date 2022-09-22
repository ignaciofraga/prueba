# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import numpy
import pandas
import datetime

import psycopg2
import pandas.io.sql as psql


import matplotlib.pyplot as plt

from dateutil.relativedelta import relativedelta
import matplotlib.dates as mdates


# Parámetros de la base de datos
base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

num_meses              = 6
tiempo_final_consulta  = datetime.date.today()
tiempo_inicio_consulta = tiempo_final_consulta + relativedelta(months=-num_meses)

fechas_referencia = pandas.date_range(tiempo_inicio_consulta,tiempo_final_consulta,freq='m')

# Recupera la tabla de los programas disponibles como un dataframe
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
df_programas = psql.read_sql('SELECT * FROM programas', conn)

# Recupera la tabla del estado de los procesos como un dataframe
temporal_estado_procesos = psql.read_sql('SELECT * FROM estado_procesos', conn)
conn.close()

# Estados
nombre_estados  = ['No disponible','Pendiente de análisis','Analizado','Post-Procesado']
colores_estados = ['#CD5C5C','#F4A460','#87CEEB','#66CDAA','#2E8B57']



# Contador
num_valores = numpy.zeros((num_meses,df_programas.shape[0],len(nombre_estados)),dtype=int)

for itiempo in range(num_meses):
    
    tiempo_consulta = fechas_referencia[itiempo]
    
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
fechas_referencia = pandas.date_range(tiempo_inicio_consulta,tiempo_final_consulta,freq='m')
anchura_barra     = 4
etiquetas         = df_programas['abreviatura']

# Bucle con cada programa de muestreo
valor_maximo_programa = numpy.zeros(df_programas.shape[0])
for iprograma in range(df_programas.shape[0]): 
    
    # Desplaza las fechas para representar cada programa separado de los demás
    for ifecha in range(fechas_referencia.shape[0]):
        fechas_programa    = fechas_referencia + datetime.timedelta(days=anchura_barra*iprograma)
    
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
        
        plt.bar(fechas_programa, num_valores[:,iprograma,igrafico], anchura_barra, bottom = posicion_fondo ,color=colores_estados[igrafico],edgecolor='k')

    # Añade una etiqueta para identificar al programa
    etiqueta_nombre   = [etiquetas[iprograma]]*num_valores.shape[0]
    for ibarra in range(num_valores.shape[0]):
        # Etiqueta con el nombre del programa
        ax.text(fechas_programa[ibarra], etiqueta_altura[ibarra], etiqueta_nombre[ibarra], ha="center", va="bottom")
        # Etiqueta con el valor de cada uno de los estados
        if valores_programa[ibarra,0] > 0:
            ax.text(fechas_programa[ibarra], valores_acumulados[ibarra,0] , str(valores_programa[ibarra,0]), ha="center", va="bottom")
        if valores_programa[ibarra,1] > 0:
            ax.text(fechas_programa[ibarra], valores_acumulados[ibarra,1] , str(valores_programa[ibarra,1]), ha="center", va="bottom")
        if valores_programa[ibarra,2] > 0:
            ax.text(fechas_programa[ibarra], valores_acumulados[ibarra,2] , str(valores_programa[ibarra,2]), ha="center", va="bottom")
        if valores_programa[ibarra,3] > 0:
            ax.text(fechas_programa[ibarra], valores_acumulados[ibarra,3] , str(valores_programa[ibarra,3]), ha="center", va="bottom")




# Ajusta límites del gráfico
plt.ylim([0, max(valor_maximo_programa)+2])
     
# Ajusta tamaño 
fig.set_size_inches(16.5, 5.5)

# Cambia el formato de los ejes
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))

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