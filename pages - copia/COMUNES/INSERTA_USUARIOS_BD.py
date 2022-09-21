# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 17:55:43 2022

@author: Nacho
"""

base_datos     = 'COAC'
usuario        = 'postgres'
contrasena     = 'm0nt34lt0'
puerto         = '5432'
direccion_host = '193.146.155.99'

import psycopg2


##########################################
## TABLA CON LOS USUARIOS Y CONTRASEÑAS ##
##########################################

conn = psycopg2.connect(host = direccion_host,database=base_datos, user=usuario, password=contrasena, port=puerto)
cursor = conn.cursor()

nombre_tabla = 'usuarios_app'

# Borra la table si ya existía
instruccion_sql = 'DROP TABLE IF EXISTS ' + nombre_tabla + ' CASCADE;'
cursor.execute(instruccion_sql)
conn.commit()

# Crea la tabla de nuevo
listado_variables = ('(id_usuario SERIAL PRIMARY KEY,'
' nombre_usuario text NOT NULL,'
' password text NOT NULL'
) 

listado_unicidades = (', UNIQUE (nombre_usuario,password))')

instruccion_sql = 'CREATE TABLE IF NOT EXISTS ' + nombre_tabla + ' ' + listado_variables + ' ' + listado_unicidades
cursor.execute(instruccion_sql)
conn.commit()


# Inserta los datos de usuarios y contraseñas
##### ESTOS VALORES TIENEN QUE SER LOS MISMOS QUE EN LA APP STREAMLIT ######

usuarios   = ['Usuario interno IEO','Usuario externo']
contrasena = ['IEO_2022','2022_IEO']

for iusuario in range(len(usuarios)):
    instruccion_sql = "INSERT INTO usuarios_app (nombre_usuario,password) VALUES (%s,%s) ON CONFLICT (id_usuario) DO UPDATE SET (nombre_usuario,password) = (EXCLUDED.nombre_usuario, EXCLUDED.password);"   
    cursor.execute(instruccion_sql, (usuarios[iusuario],contrasena[iusuario]))
    conn.commit() 

cursor.close()
conn.close()





