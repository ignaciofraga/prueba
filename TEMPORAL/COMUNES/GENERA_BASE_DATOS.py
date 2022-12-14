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


##########################################
## TABLA CON LOS CENTROS OCEANOGRÁFICOS ##
##########################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'centros_oceanograficos'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_centro int PRIMARY KEY,'
' nombre_centro text NOT NULL'
) 

listado_unicidades = (', UNIQUE (nombre_centro))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()



#####################################
## TABLA CON LOS BUQUES UTILIZADOS ##
#####################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'buques'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_buque int PRIMARY KEY,'
' nombre_buque text NOT NULL,'
' codigo_buque text NOT NULL'
) 

listado_unicidades = (', UNIQUE (nombre_buque,codigo_buque))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades

cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


############################################
## TABLA CON LOS PROGRAMAS DE OBSERVACION ##
############################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'programas'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_programa int PRIMARY KEY,'
' nombre_programa text NOT NULL,'
' centro_asociado int NOT NULL,'
) 

listado_dependencias = ('FOREIGN KEY (centro_asociado)'
  'REFERENCES centros_oceanograficos (id_centro)'
  ' ON UPDATE CASCADE '
  'ON DELETE CASCADE')

listado_unicidades = (', UNIQUE (nombre_programa,centro_asociado))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()



#########################################################
## TABLA CON LOS ESTADOS DEL PROCESADO DE LAS CAMPAÑAS ##
#########################################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'estado_procesos'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_proceso SERIAL PRIMARY KEY,'
' programa int NOT NULL,'
' nombre_programa text NOT NULL,'
' año int NOT NULL,'
' fecha_final_muestreo date,'
' fecha_entrada_datos date,'
' fecha_analisis_laboratorio date,'
' fecha_post_procesado date,'
' contacto_muestreo text,'
' contacto_entrada_datos text,'
' contacto_analisis_laboratorio text,'
' contacto_post_procesado text,'
) 

listado_dependencias = ('FOREIGN KEY (programa)'
  'REFERENCES programas (id_programa)'
  ' ON UPDATE CASCADE '
  'ON DELETE CASCADE')

listado_unicidades = (', UNIQUE (programa,año))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


##################################################
## TABLA CON LAS CONFIGURACIONES DEL PERFILADOR ##
##################################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'configuracion_perfilador'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_config_perfil int PRIMARY KEY,'
' buque int NOT NULL,'
' centro_asociado int,'
' fecha_inicio date,'
' sensor_ctd text,'
' num_serie_ctd text,'
' propietario_ctd text,'
' fecha_calibracion_ctd date,'
' ruta_configuracion_ctd text,'
' sensor_par text,'
' num_serie_par text,'
' fecha_calibracion_par date,'
' sensor_oxigeno text,'
' num_serie_oxigeno text,'
' fecha_calibracion_oxigeno date,'
' sensor_fluorescencia text,'
' num_serie_fluorescencia text,'
' fecha_calibracion_fluorescencia date,'
' adcp text,'
' num_serie_adcp text,'
' fecha_calibracion_adcp date,'
) 

listado_dependencias = ('FOREIGN KEY (centro_asociado)'
  'REFERENCES centros_oceanograficos (id_centro)'
  ' ON UPDATE CASCADE '
  'ON DELETE CASCADE,'
  'FOREIGN KEY (buque)'
  'REFERENCES buques (id_buque)'
  'ON UPDATE CASCADE ON DELETE CASCADE'
  )

listado_unicidades = (', UNIQUE (buque,centro_asociado,fecha_inicio,sensor_ctd,num_serie_ctd,sensor_par,num_serie_par,sensor_oxigeno,num_serie_oxigeno,sensor_fluorescencia,num_serie_fluorescencia,adcp,num_serie_adcp))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


##############################################################
## TABLA CON LAS CONFIGURACIONES DEL MUESTREO EN SUPERFICIE ##
##############################################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'configuracion_superficie'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_config_superficie int PRIMARY KEY,'
' buque int NOT NULL,'
' centro_asociado int,'
' fecha_inicio date,'
' sensor_tsg text,'
' num_serie_tsg text,'
' propietario_tsg text,'
' fecha_calibracion_tsg date,'
' ruta_configuracion_tsg text,'
' sensor_ph text,'
' num_serie_ph text,'
' fecha_calibracion_ph date,'
' sensor_oxigeno text,'
' num_serie_oxigeno text,'
' fecha_calibracion_oxigeno date,'
' sensor_fluorescencia text,'
' num_serie_fluorescencia text,'
' fecha_calibracion_fluorescencia date,'
' sensor_pco2 text,'
' num_serie_pco2 text,'
' fecha_calibracion_pco2 date,'
) 

listado_dependencias = ('FOREIGN KEY (centro_asociado)'
  'REFERENCES centros_oceanograficos (id_centro)'
  ' ON UPDATE CASCADE '
  'ON DELETE CASCADE,'
  'FOREIGN KEY (buque)'
  'REFERENCES buques (id_buque)'
  'ON UPDATE CASCADE ON DELETE CASCADE'
  )


listado_unicidades = (', UNIQUE (buque,centro_asociado,fecha_inicio,sensor_tsg,num_serie_tsg,sensor_ph,num_serie_ph,sensor_oxigeno,num_serie_oxigeno,sensor_fluorescencia,num_serie_fluorescencia,sensor_pco2,num_serie_pco2))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


##########################################
## TABLA CON LAS ESTACIONES MUESTREADAS ##
##########################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'estaciones'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

listado_variables = ('(id_estacion SERIAL PRIMARY KEY,'
' nombre_estacion text NOT NULL,'
' programa int NOT NULL,'
' latitud NUMERIC (6, 4) NOT NULL,'
' longitud NUMERIC (6, 4) NOT NULL,'
) 

listado_dependencias = ('FOREIGN KEY (programa)'
'REFERENCES programas (id_programa)'
'ON UPDATE CASCADE ON DELETE CASCADE')

listado_unicidades = (', UNIQUE (programa,latitud,longitud))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()



########################################################
## TABLA CON LOS TRANSECTOS EN SUPERFICIE MUESTREADOS ##
########################################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'transectos_superficie'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

listado_variables = ('(id_transecto SERIAL PRIMARY KEY,'
' nombre_transecto text,'
' estacion_inicio int NOT NULL,'
' estacion_final int NOT NULL,'
' fecha_inicio timestamptz NOT NULL,'
' fecha_final timestamptz,'
' configuracion_muestreo_superficie int NOT NULL,'
) 

listado_dependencias = ('FOREIGN KEY (estacion_inicio)'
'REFERENCES estaciones (id_estacion)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (estacion_final)'
'REFERENCES estaciones (id_estacion)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (configuracion_muestreo_superficie)'
'REFERENCES configuracion_superficie (id_config_superficie)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (estacion_inicio,estacion_final,fecha_inicio,configuracion_muestreo_superficie))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()



####################################################
## TABLA CON LOS PERFILES EN VERTICAL MUESTREADOS ##
####################################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'perfiles_verticales'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

listado_variables = ('(id_perfil SERIAL PRIMARY KEY,'
' nombre_perfil text,'
' estacion int NOT NULL,'
' feha_perfil timestamptz NOT NULL,'
' configuracion_perfilador int NOT NULL,'
) 

listado_dependencias = ('FOREIGN KEY (estacion)'
'REFERENCES estaciones (id_estacion)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (configuracion_perfilador)'
'REFERENCES configuracion_perfilador (id_config_perfil)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (estacion,feha_perfil,configuracion_perfilador))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()




#############################################################
## TABLA CON LOS MUESTREOS DISCRETOS EN LA COLUMNA DE AGUA ##
#############################################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'muestreos_discretos'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_muestreo SERIAL PRIMARY KEY,'
' nombre_muestreo text,'
' estacion int NOT NULL,'
' fecha_muestreo date NOT NULL,'
' hora_muestreo time,'
' presion_ctd NUMERIC (6, 2) NOT NULL,'
' botella int,'
' num_cast int,'
' configuracion_perfilador int NOT NULL,'
' configuracion_superficie int NOT NULL,'
' id_tubo_nutrientes int,'
) 

listado_dependencias = ('FOREIGN KEY (estacion)'
'REFERENCES estaciones (id_estacion)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (configuracion_perfilador)'
'REFERENCES configuracion_perfilador (id_config_perfil)'
'ON UPDATE CASCADE ON DELETE CASCADE,'
'FOREIGN KEY (configuracion_superficie)'
'REFERENCES configuracion_superficie (id_config_superficie)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (estacion,fecha_muestreo,hora_muestreo,presion_ctd,configuracion_superficie,configuracion_perfilador))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades


cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()




















#### TABLAS CON DATOS MUESTREADOS ####


#######################################################################
## TABLA CON DATOS BIOGEOQUÍMICOS PROCEDENTES DE MUESTREOS PUNTUALES ##
#######################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_discretos_biogeoquimica'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_disc_biogeoquim SERIAL PRIMARY KEY,'
' muestreo int NOT NULL,'
' fluorescencia_ctd NUMERIC (7, 4),'
' fluorescencia_ctd_qf int DEFAULT 9,'
' oxigeno_ctd NUMERIC (4, 1),'
' oxigeno_ctd_qf int DEFAULT 9,'
' oxigeno_wk NUMERIC (4, 1),'
' oxigeno_wk_qf int DEFAULT 9,'
' no3 NUMERIC (5, 2),'
' no3_qf int DEFAULT 9,'
' no2 NUMERIC (5, 2),'
' no2_qf int DEFAULT 9,'
' nh4 NUMERIC (5, 2),'
' nh4_qf int DEFAULT 9,'
' po4 NUMERIC (5, 2),'
' po4_qf int DEFAULT 9,'
' sio2 NUMERIC (5, 2),'
' sio2_qf int DEFAULT 9,'
' tcarbn NUMERIC (6, 1),'
' tcarbn_qf int DEFAULT 9,'
' doc NUMERIC (6, 3),'
' doc_qf int DEFAULT 9,'
' cdom NUMERIC (6, 3),'
' cdom_qf int DEFAULT 9,'
' clorofila_a NUMERIC (5, 3),'
' clorofila_a_qf int DEFAULT 9,'
' alkali NUMERIC (9, 4),'
' alkali_qf int DEFAULT 9,'
' phts25P0_unpur NUMERIC (5, 4),'
' phts25P0_unpur_qf int DEFAULT 9,'
' phts25P0_pur NUMERIC (5, 4),'
' phts25P0_pur_qf int DEFAULT 9,'
' r_clor NUMERIC (10, 8),'
' r_clor_qf int DEFAULT 9,'
' r_per NUMERIC (10, 8),'
' r_per_qf int DEFAULT 9,'
' co3_temp NUMERIC (3, 1),'
) 

listado_dependencias = ('FOREIGN KEY (muestreo)'
'REFERENCES muestreos_discretos (id_muestreo)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (muestreo))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()



################################################################
## TABLA CON DATOS FISICOS PROCEDENTES DE MUESTREOS PUNTUALES ##
################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_discretos_fisica'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_disc_fisica SERIAL PRIMARY KEY,'
' muestreo int NOT NULL,'
' temperatura_ctd NUMERIC (4, 2),'
' temperatura_ctd_qf int DEFAULT 9,'
' salinidad_ctd NUMERIC (5, 3),'
' salinidad_ctd_qf int DEFAULT 9,'
' par_ctd NUMERIC (8, 3),'
' par_ctd_qf int DEFAULT 9,'
' turbidez_ctd NUMERIC (6, 3),'
' turbidez_ctd_qf int DEFAULT 9,'
) 

listado_dependencias = ('FOREIGN KEY (muestreo)'
'REFERENCES muestreos_discretos (id_muestreo)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (muestreo))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


#####################################################################
## TABLA CON DATOS FISICOS PROCEDENTES DE TRANSECTOS SUPERFICIALES ##
#####################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_superficie_fisica'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_sup_fisica SERIAL PRIMARY KEY,'
' transecto int NOT NULL,'
' temperatura_sup json,'
' salinidad_sup json,'
' turbidez_sup json,'
) 

listado_dependencias = ('FOREIGN KEY (transecto)'
'REFERENCES transectos_superficie (id_transecto)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (transecto))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


############################################################################
## TABLA CON DATOS BIOGEOQUIMICOS PROCEDENTES DE TRANSECTOS SUPERFICIALES ##
############################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_superficie_biogeoquimica'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_sup_biogeoquim SERIAL PRIMARY KEY,'
' transecto int NOT NULL,'
' ph_sup json,'
' pco2_sup json,'
' fluorescencia_sup json,'
' oxigeno_sup json,'
) 

listado_dependencias = ('FOREIGN KEY (transecto)'
'REFERENCES transectos_superficie (id_transecto)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (transecto))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()



################################################################
## TABLA CON DATOS FISICOS PROCEDENTES DE PERFILES VERTICALES ##
################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_perfil_fisica'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_perfil_fisica SERIAL PRIMARY KEY,'
' perfil int NOT NULL,'
' temperatura_perfil json,'
' salinidad_perfil json,'
' par_perfil json,'
' veloc_x json,'
' veloc_y json,'
' veloc_z json,'
) 

listado_dependencias = ('FOREIGN KEY (perfil)'
'REFERENCES perfiles_verticales (id_perfil)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (perfil))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()


#######################################################################
## TABLA CON DATOS BIOGEOQUIMICOS PROCEDENTES DE PERFILES VERTICALES ##
#######################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'datos_perfil_biogeoquimica'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_perfil_biogeoquim SERIAL PRIMARY KEY,'
' perfil int NOT NULL,'
' oxigeno_perfil json,'
' fluorescencia_perfil json,'
) 

listado_dependencias = ('FOREIGN KEY (perfil)'
'REFERENCES perfiles_verticales (id_perfil)'
'ON UPDATE CASCADE ON DELETE CASCADE'
)

listado_unicidades = (', UNIQUE (perfil))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_dependencias + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()




##############################################################################
## TABLA CON LA RELACION DE VARIABLES UTILIZADAS EN LA IMPORTACION DE DATOS ##
##############################################################################
conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'variables_procesado'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ';'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_variable SERIAL PRIMARY KEY,'
'variables_biogeoquimicas text,'
'variables_fisicas text,'
' parametros_muestreo text)'
) 

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables
cursor.execute(instruccion_sql)
conn.commit()
cursor.close()
conn.close()

