import pandas as pd
import requests
from datetime import datetime, timedelta
import mysql.connector
from IPython.core.interactiveshell import InteractiveShell 
InteractiveShell.ast_node_interactivity = "all" 
import soporte as patata #En honor a Ana García as MátameCamión



## Abrimos el archivo del censo:
df_censo_ccaa = pd.read_csv("data/poblacion_comunidades.csv", index_col = 0)

#Llamamos a la clase Extraccion para sacar los datos de la API:
energias = patata.Extraccion(2011, 2022)

#Extraemos los datos de las energías a nivel nacional:
df_nacional = energias.nacional()

#Extraemos los datos de las energías a nivel autonómico:
df_ccaa = energias.ccaa(sp.cod_comunidades) 

#Limpiamos el df_censo_ccaa:
energias.limpieza_censo(df_censo_ccaa)

#Limpiamos el df_nacional y el df_ccaa:
energias.limpieza_energias(df_nacional)
energias.limpieza_energias(df_ccaa)

#Llamamos a la clase CrearBBDD:
bbdd = patata.CrearBBDD("energia", "AlumnaAdalab")

#Creamos primero la base de datos:
bbdd.creacion_bbdd()

#A continuación creamos las diferentes tablas:
bbdd.creacion_insercion_tabla(patata.tabla_comunidades)
bbdd.creacion_insercion_tabla(patata.tabla_fechas)
bbdd.creacion_insercion_tabla(patata.tabla_energia_comunidades)
bbdd.creacion_insercion_tabla(patata.tabla_energia_nacional)