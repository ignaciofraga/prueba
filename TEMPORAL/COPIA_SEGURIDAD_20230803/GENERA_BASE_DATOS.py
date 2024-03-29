# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 11:56:39 2022

@author: ifraga
"""

# Carga de módulos

import psycopg2


# DATOS COMUNES
# base_datos     = 'IEO_Coruna'
# usuario        = 'postgres'
# contrasena     = 'IEO2022'
# puerto         = '5432'
# direccion_host = 'localhost'


base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'


#### TABLAS CON INFORMACIÓN DE LAS CONDICIONES DE MUESTREO (EQUIV. METADATOS) ####


# ##########################################
# ## TABLA CON LOS CENTROS OCEANOGRÁFICOS ##
# ##########################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'centros_oceanograficos'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_centro int PRIMARY KEY,'
# ' nombre_centro text NOT NULL'
# ) 

# listado_unicidades = (', UNIQUE (nombre_centro))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# #####################################
# ## TABLA CON LOS BUQUES UTILIZADOS ##
# #####################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'buques'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_buque int PRIMARY KEY,'
# ' nombre_buque text NOT NULL,'
# ' codigo_buque text NOT NULL'
# ) 

# listado_unicidades = (', UNIQUE (nombre_buque,codigo_buque))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades

# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# ############################################
# ## TABLA CON LOS PROGRAMAS DE OBSERVACION ##
# ############################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'programas'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_programa int PRIMARY KEY,'
# ' nombre_programa text NOT NULL,'
# ' centro_asociado int NOT NULL,'
# ' abreviatura text NOT NULL,'

# ) 

# listado_dependencias = ('FOREIGN KEY (centro_asociado)'
#   'REFERENCES centros_oceanograficos (id_centro)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE')

# listado_unicidades = (', UNIQUE (nombre_programa,centro_asociado,abreviatura))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# #########################################################
# ## TABLA CON LOS ESTADOS DEL PROCESADO DE LAS CAMPAÑAS ##
# #########################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'estado_procesos'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_proceso SERIAL PRIMARY KEY,'
# ' programa int NOT NULL,'
# ' nombre_programa text NOT NULL,'
# ' año int NOT NULL,'
# ' fecha_final_muestreo date,'
# ' fecha_analisis_laboratorio date,'
# ' fecha_post_procesado date,'
# ' contacto_muestreo text,'
# ' contacto_post_procesado text,'
# ) 

# listado_dependencias = ('FOREIGN KEY (programa)'
#   'REFERENCES programas (id_programa)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE')

# listado_unicidades = (', UNIQUE (programa,año))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()








# ##################################################
# ## TABLA CON LAS CONFIGURACIONES DEL PERFILADOR ##
# ##################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'configuracion_perfilador'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_config_perfil int PRIMARY KEY,'
# ' buque int NOT NULL,'
# ' ctd text,'
# ' num_serie_ctd text,'
# ' sensor_par text,'
# ' num_serie_par text,'
# ' fecha_calibracion_par date,'
# ' sensor_oxigeno text,'
# ' num_serie_oxigeno text,'
# ' fecha_calibracion_oxigeno date,'
# ' sensor_fluorescencia text,'
# ' num_serie_fluorescencia text,'
# ' fecha_calibracion_fluorescencia date,'
# ' sensor_conductividad text,'
# ' num_serie_conductividad text,'
# ' fecha_calibracion_conductividad date,'
# ' sensor_presion text,'
# ' num_serie_presion text,'
# ' fecha_calibracion_presion date,'
# ' sensor_turbidez text,'
# ' num_serie_turbidez text,'
# ' fecha_calibracion_turbidez date,'
# ) 

# listado_dependencias = ('FOREIGN KEY (buque)'
#   'REFERENCES buques (id_buque)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )


# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias 


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# ##############################################################
# ## TABLA CON LAS CONFIGURACIONES DEL MUESTREO EN SUPERFICIE ##
# ##############################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'configuracion_superficie'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_config_superficie int PRIMARY KEY,'
# ' buque int NOT NULL,'
# ' sensor_tsg text,'
# ' num_serie_tsg text,'
# ' fecha_calibracion_tsg date,'
# ' sensor_ph text,'
# ' num_serie_ph text,'
# ' fecha_calibracion_ph date,'
# ' sensor_oxigeno text,'
# ' num_serie_oxigeno text,'
# ' fecha_calibracion_oxigeno date,'
# ' sensor_fluorescencia text,'
# ' num_serie_fluorescencia text,'
# ' fecha_calibracion_fluorescencia date,'
# ' sensor_pco2 text,'
# ' num_serie_pco2 text,'
# ' fecha_calibracion_pco2 date,'
# ) 

# listado_dependencias = ('FOREIGN KEY (buque)'
#   'REFERENCES buques (id_buque)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )


# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias 


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# ##########################################
# ## TABLA CON LAS ESTACIONES MUESTREADAS ##
# ##########################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'estaciones'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# listado_variables = ('(id_estacion SERIAL PRIMARY KEY,'
# ' nombre_estacion text NOT NULL,'
# ' programa int NOT NULL,'
# ' latitud_estacion NUMERIC (6, 4),'
# ' longitud_estacion NUMERIC (6, 4),'
# ' profundidades_referencia json,'
# ) 

# listado_dependencias = ('FOREIGN KEY (programa)'
# 'REFERENCES programas (id_programa)'
# 'ON UPDATE CASCADE ON DELETE CASCADE')

# listado_unicidades = (', UNIQUE (programa,nombre_estacion))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# ########################################################
# ## TABLA CON LOS TRANSECTOS EN SUPERFICIE MUESTREADOS ##
# ########################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'transectos_superficie'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# listado_variables = ('(id_transecto SERIAL PRIMARY KEY,'
# ' nombre_transecto text,'
# ' estacion_inicio int NOT NULL,'
# ' estacion_final int NOT NULL,'
# ' fecha_inicio timestamptz NOT NULL,'
# ' fecha_final timestamptz,'
# ' configuracion_muestreo_superficie int NOT NULL,'
# ) 

# listado_dependencias = ('FOREIGN KEY (estacion_inicio)'
# 'REFERENCES estaciones (id_estacion)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (estacion_final)'
# 'REFERENCES estaciones (id_estacion)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (configuracion_muestreo_superficie)'
# 'REFERENCES configuracion_superficie (id_config_superficie)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (estacion_inicio,estacion_final,fecha_inicio,configuracion_muestreo_superficie))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# ####################################################
# ## TABLA CON LOS PERFILES EN VERTICAL MUESTREADOS ##
# ####################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'perfiles_verticales'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# listado_variables = ('(perfil SERIAL PRIMARY KEY,'
# ' nombre_perfil text,'
# ' salida_mar int NOT NULL,'
# ' estacion int NOT NULL,'
# ' latitud_muestreo NUMERIC (6, 4) NOT NULL,'
# ' longitud_muestreo NUMERIC (6, 4) NOT NULL,'
# ' num_cast int,'
# ' fecha_perfil date NOT NULL,'
# ' hora_perfil time,'
# ' configuracion_perfilador int NOT NULL,'
# ) 

# listado_dependencias = ('FOREIGN KEY (estacion)'
# 'REFERENCES estaciones (id_estacion)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (salida_mar)'
# 'REFERENCES salidas_muestreos (id_salida)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (configuracion_perfilador)'
# 'REFERENCES configuracion_perfilador (id_config_perfil)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (estacion,fecha_perfil,num_cast))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# ####################################################################
# ## TABLA CON LAS SALIDAS REALIZADAS (ENFOCADO AL PROGRAMA RADIAL) ##
# ####################################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'salidas_muestreos'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_salida SERIAL PRIMARY KEY,'
# ' nombre_salida text NOT NULL,'
# ' programa int NOT NULL,'
# ' nombre_programa text NOT NULL,'
# ' tipo_salida text,'
# ' fecha_salida date NOT NULL,'
# ' hora_salida time,'
# ' fecha_retorno date NOT NULL,'
# ' hora_retorno time,'
# ' buque int,'
# ' configuracion_perfilador int,'
# ' configuracion_superficie int,'
# ' estaciones json,'
# ' participantes_comisionados json,'
# ' participantes_no_comisionados json,'
# ' variables_muestreadas json,'
# ' observaciones text,'
# ) 

# listado_dependencias = ('FOREIGN KEY (programa)'
#   'REFERENCES programas (id_programa)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE,'
#   'FOREIGN KEY (buque)'
#   'REFERENCES buques (id_buque)'
#   'ON UPDATE CASCADE ON DELETE CASCADE,'
#   'FOREIGN KEY (configuracion_perfilador)'
#   'REFERENCES configuracion_perfilador (id_config_perfil)'
#   'ON UPDATE CASCADE ON DELETE CASCADE,'
#   'FOREIGN KEY (configuracion_superficie)'
#   'REFERENCES configuracion_superficie (id_config_superficie)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )


# listado_unicidades = (', UNIQUE (programa,fecha_salida))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# #############################################################
# ## TABLA CON LOS MUESTREOS DISCRETOS EN LA COLUMNA DE AGUA ##
# #############################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'muestreos_discretos'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(muestreo int PRIMARY KEY,'
# ' nombre_muestreo text,'
# ' id_externo text,'
# ' fecha_muestreo date NOT NULL,'
# ' hora_muestreo time,'
# ' latitud_muestreo NUMERIC (6, 4),'
# ' longitud_muestreo NUMERIC (6, 4),'
# ' salida_mar int,'
# ' estacion int NOT NULL,'
# ' num_cast int,'
# ' botella int,'
# ' prof_referencia NUMERIC (6, 2),'
# ' presion_ctd NUMERIC (6, 2),'
# ) 

# listado_dependencias = ('FOREIGN KEY (estacion)'
# 'REFERENCES estaciones (id_estacion)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (salida_mar)'
# 'REFERENCES salidas_muestreos (id_salida)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (estacion,fecha_muestreo,hora_muestreo,salida_mar,presion_ctd,botella))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()








# ### TABLAS CON TIPOS DE PROCESADO

# ## TABLA CON CONTROL CALIDAD APLICADO A DATO DE NUTRIENTE ##

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'control_calidad_nutrientes'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# listado_variables = ('(id_control int PRIMARY KEY,'
# ' tipo_control text NOT NULL,'
# ) 

# listado_unicidades = (' UNIQUE (id_control))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# ## TABLA CON CONTROL CALIDAD APLICADO A DATO DE NUTRIENTE ##

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'metodo_pH'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# listado_variables = ('(id_metodo int PRIMARY KEY,'
# ' metodo_ph text NOT NULL,'
# ' descripcion_metodo_ph text NOT NULL,'
# ) 

# listado_unicidades = (' UNIQUE (id_metodo))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()











# ### TABLAS CON DATOS MUESTREADOS ####

########################################################
## TABLA CON DATOS PROCEDENTES DE MUESTREOS PUNTUALES ##
########################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_discretos'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(muestreo int,'
' temperatura_ctd NUMERIC (4, 2),'
' temperatura_ctd_qf int DEFAULT 9,'
' salinidad_ctd NUMERIC (5, 3),'
' salinidad_ctd_qf int DEFAULT 9,'
' par_ctd NUMERIC (8, 3),'
' par_ctd_qf int DEFAULT 9,'
' turbidez_ctd NUMERIC (6, 3),'
' turbidez_ctd_qf int DEFAULT 9,'
' sigmat NUMERIC (6, 4),'
' fluorescencia_ctd NUMERIC (7, 4),'
' fluorescencia_ctd_qf int DEFAULT 9,'
' oxigeno_ctd NUMERIC (4, 1),'
' oxigeno_ctd_qf int DEFAULT 9,'
' oxigeno_wk NUMERIC (4, 1),'
' oxigeno_wk_qf int DEFAULT 9,'
' ton NUMERIC (5, 2),'
' ton_qf int DEFAULT 9,'
' nitrato NUMERIC (5, 2),'
' nitrato_qf int DEFAULT 9,'
' nitrito NUMERIC (5, 2),'
' nitrito_qf int DEFAULT 9,'
' amonio NUMERIC (5, 2),'
' amonio_qf int DEFAULT 9,'
' fosfato NUMERIC (5, 2),'
' fosfato_qf int DEFAULT 9,'
' silicato NUMERIC (5, 2),'
' silicato_qf int DEFAULT 9,'
' cc_nutrientes int DEFAULT 1,'
' tcarbn NUMERIC (6, 1),'
' tcarbn_qf int DEFAULT 9,'
' doc NUMERIC (6, 3),'
' doc_qf int DEFAULT 9,'
' cdom NUMERIC (6, 3),'
' cdom_qf int DEFAULT 9,'
' clorofila_a NUMERIC (5, 3),'
' clorofila_a_qf int DEFAULT 9,'
' clorofila_b NUMERIC (5, 3),'
' clorofila_b_qf int DEFAULT 9,'
' clorofila_c NUMERIC (5, 3),'
' clorofila_c_qf int DEFAULT 9,'
' alcalinidad NUMERIC (9, 4),'
' alcalinidad_qf int DEFAULT 9,'
' ph NUMERIC (5, 4),'
' ph_qf int DEFAULT 9,'
' ph_metodo int DEFAULT 1,'
' prod_primaria NUMERIC (5, 2),'
' r_clor NUMERIC (10, 8),'
' r_clor_qf int DEFAULT 9,'
' r_per NUMERIC (10, 8),'
' r_per_qf int DEFAULT 9,'
' co3_temp NUMERIC (3, 1),'
' cop NUMERIC (5, 2),'
' nop NUMERIC (4, 2),'
' rmn_alto_procesado int,'
' rmn_bajo_procesado int,'
' temp_lab_procesado int,'
' rto_columna_procesado NUMERIC (4, 2),'
' tubo_nutrientes text,'
) 

listado_dependencias = ('FOREIGN KEY (muestreo)'
'REFERENCES muestreos_discretos (muestreo)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (cc_nutrientes)'
'REFERENCES control_calidad_nutrientes (id_control)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (pH_metodo)'
'REFERENCES metodo_pH (id_metodo)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (rmn_alto_procesado)'
'REFERENCES rmn_alto_nutrientes (id_rmn)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (rmn_bajo_procesado)'
'REFERENCES rmn_bajo_nutrientes (id_rmn)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (muestreo))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


# #######################################################################
# ## TABLA CON DATOS BIOGEOQUÍMICOS PROCEDENTES DE MUESTREOS PUNTUALES ##
# #######################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'datos_discretos_biogeoquimica'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(muestreo int,'
# ' fluorescencia_ctd NUMERIC (7, 4),'
# ' fluorescencia_ctd_qf int DEFAULT 9,'
# ' oxigeno_ctd NUMERIC (4, 1),'
# ' oxigeno_ctd_qf int DEFAULT 9,'
# ' oxigeno_wk NUMERIC (4, 1),'
# ' oxigeno_wk_qf int DEFAULT 9,'
# ' ton NUMERIC (5, 2),'
# ' ton_qf int DEFAULT 9,'
# ' nitrato NUMERIC (5, 2),'
# ' nitrato_qf int DEFAULT 9,'
# ' nitrito NUMERIC (5, 2),'
# ' nitrito_qf int DEFAULT 9,'
# ' amonio NUMERIC (5, 2),'
# ' amonio_qf int DEFAULT 9,'
# ' fosfato NUMERIC (5, 2),'
# ' fosfato_qf int DEFAULT 9,'
# ' silicato NUMERIC (5, 2),'
# ' silicato_qf int DEFAULT 9,'
# ' cc_nutrientes int DEFAULT 1,'
# ' tcarbn NUMERIC (6, 1),'
# ' tcarbn_qf int DEFAULT 9,'
# ' doc NUMERIC (6, 3),'
# ' doc_qf int DEFAULT 9,'
# ' cdom NUMERIC (6, 3),'
# ' cdom_qf int DEFAULT 9,'
# ' clorofila_a NUMERIC (5, 3),'
# ' clorofila_a_qf int DEFAULT 9,'
# ' clorofila_b NUMERIC (5, 3),'
# ' clorofila_b_qf int DEFAULT 9,'
# ' clorofila_c NUMERIC (5, 3),'
# ' clorofila_c_qf int DEFAULT 9,'
# ' alcalinidad NUMERIC (9, 4),'
# ' alcalinidad_qf int DEFAULT 9,'
# ' ph NUMERIC (5, 4),'
# ' ph_qf int DEFAULT 9,'
# ' ph_metodo int DEFAULT 1,'
# ' prod_primaria NUMERIC (5, 2),'
# ' r_clor NUMERIC (10, 8),'
# ' r_clor_qf int DEFAULT 9,'
# ' r_per NUMERIC (10, 8),'
# ' r_per_qf int DEFAULT 9,'
# ' co3_temp NUMERIC (3, 1),'
# ' cop NUMERIC (5, 2),'
# ' nop NUMERIC (4, 2),'
# ' rmn_alto_procesado int,'
# ' rmn_bajo_procesado int,'
# ' temp_lab_procesado int,'
# ' rto_columna_procesado NUMERIC (4, 2),'
# ' tubo_nutrientes text,'
# ) 

# listado_dependencias = ('FOREIGN KEY (muestreo)'
# 'REFERENCES muestreos_discretos (muestreo)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (cc_nutrientes)'
# 'REFERENCES control_calidad_nutrientes (id_control)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (pH_metodo)'
# 'REFERENCES metodo_pH (id_metodo)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (rmn_alto_procesado)'
# 'REFERENCES rmn_alto_nutrientes (id_rmn)'
# 'ON UPDATE CASCADE ON DELETE CASCADE,'
# 'FOREIGN KEY (rmn_bajo_procesado)'
# 'REFERENCES rmn_bajo_nutrientes (id_rmn)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (muestreo))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# ################################################################
# ## TABLA CON DATOS FISICOS PROCEDENTES DE MUESTREOS PUNTUALES ##
# ################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'datos_discretos_fisica'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(muestreo int,'
# ' temperatura_ctd NUMERIC (4, 2),'
# ' temperatura_ctd_qf int DEFAULT 9,'
# ' salinidad_ctd NUMERIC (5, 3),'
# ' salinidad_ctd_qf int DEFAULT 9,'
# ' par_ctd NUMERIC (8, 3),'
# ' par_ctd_qf int DEFAULT 9,'
# ' turbidez_ctd NUMERIC (6, 3),'
# ' turbidez_ctd_qf int DEFAULT 9,'
# ' sigmat NUMERIC (6, 4),'
# ) 

# #listado_dependencias = ('FOREIGN KEY (muestreo)'
# #'REFERENCES muestreos_discretos (muestreo)'
# #)

# listado_dependencias = ('FOREIGN KEY (muestreo)'
#   'REFERENCES muestreos_discretos (muestreo)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )

# listado_unicidades = (', UNIQUE (muestreo))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# #####################################################################
# ## TABLA CON DATOS FISICOS PROCEDENTES DE TRANSECTOS SUPERFICIALES ##
# #####################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'datos_superficie_fisica'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_sup_fisica SERIAL PRIMARY KEY,'
# ' transecto int NOT NULL,'
# ' temperatura_sup json,'
# ' salinidad_sup json,'
# ' turbidez_sup json,'
# ) 

# listado_dependencias = ('FOREIGN KEY (transecto)'
# 'REFERENCES transectos_superficie (id_transecto)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (transecto))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# ############################################################################
# ## TABLA CON DATOS BIOGEOQUIMICOS PROCEDENTES DE TRANSECTOS SUPERFICIALES ##
# ############################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'datos_superficie_biogeoquimica'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_sup_biogeoquim SERIAL PRIMARY KEY,'
# ' transecto int NOT NULL,'
# ' ph_sup json,'
# ' pco2_sup json,'
# ' fluorescencia_sup json,'
# ' oxigeno_sup json,'
# ) 

# listado_dependencias = ('FOREIGN KEY (transecto)'
# 'REFERENCES transectos_superficie (id_transecto)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (transecto))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



################################################################
## TABLA CON DATOS FISICOS PROCEDENTES DE PERFILES VERTICALES ##
################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_perfiles'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(perfil int,'
' temperatura_ctd text,'
' salinidad_ctd text,'
' par_ctd text,'
' sigmat text,'
' oxigeno_ctd text,'
' fluorescencia_ctd text,'
) 

listado_dependencias = ('FOREIGN KEY (perfil)'
'REFERENCES perfiles_verticales (perfil)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (perfil))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()







# ################################################################
# ## TABLA CON DATOS FISICOS PROCEDENTES DE PERFILES VERTICALES ##
# ################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'datos_perfil_fisica'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(perfil int,'
# ' temperatura_ctd text,'
# ' salinidad_ctd text,'
# ' par_ctd text,'
# ' sigmat text,'
# ) 

# listado_dependencias = ('FOREIGN KEY (perfil)'
# 'REFERENCES perfiles_verticales (perfil)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (perfil))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()


# #######################################################################
# ## TABLA CON DATOS BIOGEOQUIMICOS PROCEDENTES DE PERFILES VERTICALES ##
# #######################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'datos_perfil_biogeoquimica'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(perfil int,'
# ' oxigeno_ctd text,'
# ' fluorescencia_ctd text,'
# ) 

# listado_dependencias = ('FOREIGN KEY (perfil)'
# 'REFERENCES perfiles_verticales (perfil)'
# 'ON UPDATE CASCADE ON DELETE CASCADE'
# )

# listado_unicidades = (', UNIQUE (perfil))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()






















### TABLAS AUXILIARES

# ################################################################
# ## TABLA CON LOS PROCESOS EN CURSO DEL SERVICIO DE NUTRIENTES ##
# ################################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'procesado_actual_nutrientes'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_proceso SERIAL PRIMARY KEY,'
# ' nombre_proceso text NOT NULL,'
# ' programa int NOT NULL,'
# ' nombre_programa text NOT NULL,'
# ' año int NOT NULL,'
# ' num_muestras int,'
# ' fecha_inicio date,'
# ' fecha_estimada_fin date,'
# ' fecha_real_fin date,'
# ' observaciones text,'
# ' io_estado int,'
# ) 

# listado_dependencias = ('FOREIGN KEY (programa)'
#   'REFERENCES programas (id_programa)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE')

# listado_unicidades = (', UNIQUE (nombre_proceso))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()





# ###############################################################
# ## TABLA CON LOS USUARIOS Y CONTRASEÑAS DE LA APLICACION WEB ##
# ###############################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'usuarios_app'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_usuario SERIAL PRIMARY KEY,'
# ' nombre_usuario text NOT NULL,'
# ' password text NOT NULL'
# ) 

# listado_unicidades = (', UNIQUE (nombre_usuario))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()




# #########################################################################
# ## TABLA CON VARIABLES USADAS PARA EL CONTROL DE CALIDAD "INFORMATICO" ##
# #########################################################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'variables_procesado'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_variable SERIAL PRIMARY KEY,'
# ' parametros_muestreo text,'
# ' variables_fisicas text,'
# ' variables_biogeoquimicas text)'
# ) 


# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables 
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()




# ###############################################
# ## TABLA CON LAS BANDERAS DE CALIDAD USADAS  ##
# ###############################################
# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'indices_calidad'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_indice SERIAL PRIMARY KEY,'
# ' indice int NOT NULL,'
# ' descripcion text)'
# ) 


# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables 
# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()














# ##########################################################
# ## TABLA CON LAS CONDICIONES AMBIENTALES DE LAS SALIDAS ##
# ##########################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'condiciones_ambientales_muestreos'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_condicion int PRIMARY KEY,'
# ' salida int NOT NULL,'
# ' estacion int NOT NULL,'
# ' hora_llegada time,'
# ' profundidad int,'
# ' nubosidad int,'
# ' lluvia text,'
# ' velocidad_viento NUMERIC (3, 1),'
# ' direccion_viento text,'
# ' pres_atmosferica int,'
# ' altura_ola NUMERIC (3, 1),'
# ' estado_mar text,'
# ' mar_fondo text,'
# ' mar_direccion text,'
# ' temp_aire NUMERIC (3, 1),'
# ' marea text,'
# ' prof_secchi NUMERIC (4, 1),'
# ' max_clorofila NUMERIC (4, 1),'
# ' humedad_relativa int,'
# ' temp_superficie NUMERIC (3, 1),'
# ) 

# listado_dependencias = ('FOREIGN KEY (salida)'
#   'REFERENCES salidas_muestreos (id_salida)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE,'
#   'FOREIGN KEY (estacion)'
#   'REFERENCES estaciones (id_estacion)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )

# listado_unicidades = (', UNIQUE (salida,estacion))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()




# #######################################################
# ## TABLA CON EL PERSONAL PARTICIPANTE EN LAS SALIDAS ##
# #######################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'personal_salidas'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_personal SERIAL PRIMARY KEY,'
# ' nombre_apellidos text NOT NULL,'
# ' correo text NOT NULL,'
# ' comisionado bool NOT NULL'
# ) 


# listado_unicidades = (', UNIQUE (nombre_apellidos))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# #############################################
# ## TABLA CON LOS ESTADILLOS DE LAS SALIDAS ##
# #############################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'estadillos_muestreo_general'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_estadillo_general int PRIMARY KEY,'
# ' salida int NOT NULL,'
# ' estacion int NOT NULL,'
# ' botella int NOT NULL,'
# ' profundidad NUMERIC (4, 1) NOT NULL,'
# ' ref_prof text NOT NULL,'
# ' id_o2 int,'
# ' id_alcalinidad int,'
# ' id_ph int,'
# ' id_nutrientes_a int,'
# ' id_nutrientes_b int,'
# ' id_toc int,'
# ' id_bacterias_flagelados int,'
# ' id_poc int,'
# ' id_clorofila int,'
# ' id_prod_primaria text,'
# ' id_ciliados text,'
# ' id_flow_cam text,'
# ' id_diversidad_microbiana text,'
# ) 

# listado_unicidades = (', UNIQUE (salida,estacion,botella))')

# listado_dependencias = ('FOREIGN KEY (salida)'
#   'REFERENCES salidas_muestreos (id_salida)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE,'
#   'FOREIGN KEY (estacion)'
#   'REFERENCES estaciones (id_estacion)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )

# instruccion_sql2 = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql2)
# conn.commit()
# cursor.close()
# conn.close()




# #############################################
# ## TABLA CON LOS ESTADILLOS DE LAS SALIDAS ##
# #############################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'estadillos_ctd'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_estadillo_ctd int PRIMARY KEY,'
# ' salida int,'
# ' estacion text,'
# ' hora_inicio time,'
# ' num_cast int,'
# ' profundidad_estimada NUMERIC (4, 1) NOT NULL,'
# ' nombre_archivo text,'
# ' observaciones text,'
# ) 


# listado_unicidades = (', UNIQUE (salida,estacion))')

# listado_dependencias = ('FOREIGN KEY (salida)'
#   ' REFERENCES salidas_muestreos (id_salida)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE'
#   )

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()





# #############################################
# ## TABLA CON LOS ESTADILLOS DE LAS SALIDAS ##
# #############################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'estadillos_continuo'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(salida int PRIMARY KEY,'
# ' estacion int NOT NULL,'
# ' hora time NOT NULL,'
# ' id_oxigeno int,'
# ' id_pH int,'
# ' id_cdom int,'
# ' id_clorofila int,'
# ' id_oxigeno int,'
# ) 


# listado_unicidades = (', UNIQUE (nombre_apellidos))')

# listado_dependencias = ('FOREIGN KEY (salida)'
#   'REFERENCES salidas_muestreos (id_salida)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE,'
#   'FOREIGN KEY (estacion)'
#   'REFERENCES estaciones (id_estacion)'
#   'ON UPDATE CASCADE ON DELETE CASCADE'
#   )

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()




# ################################################################
# ## TABLA CON LOS RMNs BAJOS UTILIZADOS EN EL ANALISIS DE NUTRIENTES ##
# ################################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'rmn_bajo_nutrientes'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_rmn SERIAL PRIMARY KEY,'
# ' nombre_rmn text NOT NULL,'
# ' salinidad NUMERIC (5, 3) NOT NULL,'
# ' ton NUMERIC (5, 3) NOT NULL,'
# ' nitrito NUMERIC (5, 3) NOT NULL,'
# ' silicato NUMERIC (5, 3) NOT NULL,'
# ' fosfato NUMERIC (5, 3) NOT NULL,'
# ' observaciones text'
# ) 

# listado_unicidades = (', UNIQUE (nombre_rmn))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades

# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()




# ################################################################
# ## TABLA CON LOS RMNs ALTOS UTILIZADOS EN EL ANALISIS DE NUTRIENTES ##
# ################################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'rmn_alto_nutrientes'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(id_rmn SERIAL PRIMARY KEY,'
# ' nombre_rmn text NOT NULL,'
# ' salinidad NUMERIC (5, 3) NOT NULL,'
# ' ton NUMERIC (5, 3) NOT NULL,'
# ' nitrito NUMERIC (5, 3) NOT NULL,'
# ' silicato NUMERIC (5, 3) NOT NULL,'
# ' fosfato NUMERIC (5, 3) NOT NULL,'
# ' observaciones text'
# ) 

# listado_unicidades = (', UNIQUE (nombre_rmn))')

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades

# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()



# ##########################################################################
# ## TABLA CON LOS FACTORES DE CORRECCION APLICADOS A DATOS DE NUTRIENTES ##
# ##########################################################################

# conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
# cursor = conn.cursor()

# nombre_tabla = 'factores_correctores_nutrientes'

# # Borra la table si ya existía
# instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
# cursor.execute(instruccion_sql)
# conn.commit()

# # Crea la tabla de nuevo
# listado_variables = ('(salida_mar int PRIMARY KEY,'
# ' nombre_salida text NOT NULL,'
# ' f_nitrato_dmin NUMERIC (7, 4),'
# ' f_nitrato_x2 NUMERIC (6, 4) NOT NULL,'
# ' f_nitrato_x NUMERIC (6, 4) NOT NULL,'
# ' f_nitrato_to NUMERIC (6, 4) NOT NULL,'
# ' f_silicato_dmin NUMERIC (7, 4),'
# ' f_silicato_x2 NUMERIC (6, 4) NOT NULL,'
# ' f_silicato_x NUMERIC (6, 4) NOT NULL,'
# ' f_silicato_to NUMERIC (6, 4) NOT NULL,'
# ' f_fosfato_dmin NUMERIC (7, 4),'
# ' f_fosfato_x2 NUMERIC (6, 4) NOT NULL,'
# ' f_fosfato_x NUMERIC (6, 4) NOT NULL,'
# ' f_fosfato_to NUMERIC (6, 4) NOT NULL,'
# ' observaciones json'
# ) 

# listado_unicidades = (', UNIQUE (salida_mar))')

# listado_dependencias = (',FOREIGN KEY (salida_mar)'
#   'REFERENCES salidas_muestreos (id_salida)'
#   ' ON UPDATE CASCADE '
#   'ON DELETE CASCADE'
#   )

# instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades

# cursor.execute(instruccion_sql)
# conn.commit()
# cursor.close()
# conn.close()
