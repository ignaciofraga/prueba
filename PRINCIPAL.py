# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 08:44:36 2022

@author: ifraga
"""

# Importa las funciones a utilizar
import streamlit as st
from PIL import Image

from FUNCIONES import FUNCIONES_AUXILIARES 
from FUNCIONES import PAGINAS

# Datos comunes utilizados por la aplicación
logo_IEO_reducido     = 'DATOS/IMAGENES/ieo.ico'

# Encabezado  
imagen_logo   = Image.open(logo_IEO_reducido)
st.set_page_config(page_title="IEO NUTRIENTES", layout="wide",page_icon=logo_IEO_reducido) 

# Si el usuario está autorizado, despliega las webs a las que tiene acceso
# if FUNCIONES_AUXILIARES.check_password() is True:
if FUNCIONES_AUXILIARES.log_in() is True:
    
    # claúsula para manetener el nombre de usuario y poder identificar qué webs desplegar
    if 'usuario' in st.session_state:

        if st.session_state["usuario"] == 'COAC - Administrador':
        
            paginas = {"INICIO": PAGINAS.principal
                        #"ENTRADA DATOS NUTRIENTES": PAGINAS.entrada_datos,
                        #"INFORMACIÓN DISPONIBLE": PAGINAS.consulta_estado
                        # "DATOS DE MUESTREOS": PAGINAS.consulta_estadillos,
                        # "GRÁFICO DE EVOLUCIÓN": PAGINAS.evolucion_analisis,
                        # "ENTRADA PROCESOS": PAGINAS.entrada_procesos_actuales,
                        # "ESTADO DEL PROCESADO DE MUESTRAS": PAGINAS.consulta_procesos
                        }
            
            
        if st.session_state["usuario"] == 'COAC - Laboratorio Química':
        
            paginas = {"INICIO": PAGINAS.principal,
                        "PROCESOS EN CURSO": PAGINAS.actualiza_procesos, 
                        "CONSULTA DATOS BOTELLAS":PAGINAS.consulta_botellas,
                        "PROCESADO NUTRIENTES":PAGINAS.procesado_nutrientes,
                        "PROCESADO QUIMICA":PAGINAS.procesado_quimica
                        }

            
        if st.session_state["usuario"] == 'COAC - Supervisión Nutrientes':
     
            paginas = {"INICIO": PAGINAS.principal,
                    "ESTADO DEL PROCESADO DE MUESTRAS": PAGINAS.consulta_procesos, 
                    "INFORMACIÓN DISPONIBLE": PAGINAS.consulta_estado,
                    "GRÁFICO DE EVOLUCIÓN": PAGINAS.evolucion_analisis
                     }           
            
        if st.session_state["usuario"] == 'Usuario externo':
            
            paginas = {"INICIO": PAGINAS.principal,
                        "ENTRADA ESTADILLOS": PAGINAS.entrada_estadillos}
            
        if st.session_state["usuario"] == 'COAC - Radiales':
            
            paginas = {"INICIO": PAGINAS.principal,
                        "SALIDAS AL MAR": PAGINAS.entrada_salidas_mar,
                        "CONDICIONES AMBIENTALES":PAGINAS.entrada_condiciones_ambientales,
                        "DATOS BOTELLAS":PAGINAS.entrada_botellas}
        
        seleccion = st.sidebar.selectbox("Elige la página a mostrar: ",tuple(paginas.keys()))
        
        paginas[seleccion]()


