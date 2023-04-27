import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector
import soporte_energia as sp

año_inicio = int(input('¿Desde que año comienza tu consulta?  '))
print('------------------------------------------')

año_final = int(input('¿En que año finaliza tu consulta?  '))
print('------------------------------------------')

api = sp.Extraccion(año_inicio, año_final)

print(f"Estamos haciendo la llamada a la API para la peninsula.")

df_peninsula = api.llamada_API_peninsula()

comunidad = input('¿De qué comunidad desea hacer su consulta?  ')
print('------------------------------------------')

while comunidad not in sp.cod_comunidades:
    print(f"La comunidad no es válida. Las comunidades son: {sp.cod_comunidades.keys()}")
    comunidad = input(f"Por favor, introduce una comunidad.  ")

print('------------------------------------------')

print(f"Estamos haciendo la llamada a la API para {comunidad}.")

codigo_com = sp.cod_comunidades[comunidad]

df_ccaa = api.llamada_API_ccaa(codigo_com)

print("-----------------------------------------")
print(f"Estamos limpiando los datos de la peninsula.")
df_peninsula = api.limpiar(df_peninsula)
print(df_peninsula)
print("-----------------------------------------")

print(f"Estamos limpiando los datos de {comunidad}.")
df_ccaa = api.limpiar(df_ccaa)
print(df_ccaa)
print("-----------------------------------------")

print(f"Estamos juntando los datos de la peninsula y {comunidad}.")
df_final = api.juntar(df_peninsula, df_ccaa)
print("-----------------------------------------")

formato = input('¿En qué formato lo deseas? csv o pickle:  ')
nombre_archivo = input('¿Que nombre quieres poner al archivo?  ')

print(f"Estamos guardando los datos.")
df_final = api.guardar(df_final, formato, nombre_archivo)
print("-----------------------------------------")
print("Tu archivo se ha guardado.")

print("-----------------------------------------")
print('Ahora vamos a cargar tus datos en una base de datos de MySQL.')
nombre_bbdd = input('¿Como quiere que se llame su base de datos?  ')
contraseña = input('Escribe su contraseña.  ')
plugin = input('Escribe su plugin.  ')

sql = sp.Cargar(nombre_bbdd, contraseña, plugin)

print("-----------------------------------------")
print('Estamos creando su base de datos.')
sql.crear_bbdd
print("-----------------------------------------")

while input('¿Quiere crear una tabla en su base de datos? Escribe SI o NO.  ') == 'SI':
    query = input('Escribe su query para crear una tabla.  ')
    sql.crear_insertar_tabla(query)
