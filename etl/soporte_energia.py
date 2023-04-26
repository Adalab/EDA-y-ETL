import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

cod_comunidades = {'Ceuta': 8744,
                    'Melilla': 8745,
                    'Andalucía': 4,
                    'Aragón': 5,
                    'Cantabria': 6,
                    'Castilla - La Mancha': 7,
                    'Castilla y León': 8,
                    'Cataluña': 9,
                    'País Vasco': 10,
                    'Principado de Asturias': 11,
                    'Comunidad de Madrid': 13,
                    'Comunidad Foral de Navarra': 14,
                    'Comunitat Valenciana': 15,
                    'Extremadura': 16,
                    'Galicia': 17,
                    'Illes Balears': 8743,
                    'Canarias': 8742,
                    'Región de Murcia': 21,
                    'La Rioja': 20}

class Extraccion: 
    def __init__(self, año_inicio, año_final):

        self.año_inicio = año_inicio
        self.año_final = año_final

    def llamada_API_peninsula(self):
        df_peninsula = pd.DataFrame()
        
        for i in range(self.año_inicio, (self.año_final + 1)):
                
                url1 = f'https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={i}-01-01T00:00&end_date={i}-12-31T23:59&time_trunc=year&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741'
                response1 = requests.get(url=url1)
                if response1.status_code != 200:
                    print(response1.reason, response1.status_code)
                    break
                
                for x in range(len(response1.json()['included'])):
                    df = pd.DataFrame(response1.json()['included'][x]['attributes']['values'])
                    df['tipo_energia'] = response1.json()['included'][x]['attributes']['title']                
                    
                    df_peninsula = pd.concat([df_peninsula, df], axis=0)
                    df_peninsula['region'] = 'Península'
                    
        return df_peninsula
        
    def llamada_API_ccaa(self, comunidad):
    
        df_ccaa = pd.DataFrame()
        
        for i in range(self.año_inicio, (self.año_final + 1)):
            
            url2 = f'https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={i}-01-01T00:00&end_date={i}-12-31T23:59&time_trunc=year&geo_trunc=electric_system&geo_limit=ccaa&geo_ids={comunidad}'
            response2 = requests.get(url=url2)
            if response2.status_code != 200:
                print(response2.reason, response2.status_code)
                break
            
            for x in range(len(response2.json()['included'])):
                df = pd.DataFrame(response2.json()['included'][x]['attributes']['values'])
                df['tipo_energia'] = response2.json()['included'][x]['attributes']['title']
                df['region'] = comunidad
                
                df_ccaa = pd.concat([df_ccaa, df], axis=0)
                    
        return df_ccaa
            
    def limpiar(self, dataframe):
        
        dataframe['value'] = round(dataframe['value'], 2)
        dataframe['percentage'] = round(dataframe['percentage'], 2)
            
        dataframe['datetime'] = dataframe['datetime'].apply(pd.to_datetime)
        
        dataframe['datetime'] = dataframe['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
        return dataframe
    
    def juntar(self, dataframe1, dataframe2):
        df_final = pd.concat([dataframe1, dataframe2], axis=0, join='outer')
        return df_final

    def guardar(self, dataframe, formato, nombre_archivo):
        if formato == 'csv':
            dataframe.to_csv(f'datos/{nombre_archivo}.csv')
        elif formato == 'pickle':
            dataframe.to_pickle(f'datos/{nombre_archivo}.pkl')

class Cargar:
    
    def __init__(self, nombre_bbdd, contraseña, plugin):
        
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña
        self.plugin = plugin

    def crear_bbdd(self):

        mydb = mysql.connector.connect(host="localhost",
                                        user="root",
                                        password=f'{self.contraseña}',
                                        auth_plugin = f'{self.plugin}')
        mycursor = mydb.cursor()

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)



    def crear_insertar_tabla(self,query):
    
        cnx = mysql.connector.connect(user='root', password=f"{self.contraseña}",
                                        host='127.0.0.1', database=f"{self.nombre_bbdd}", 
                                        auth_plugin = f'{self.plugin}')

        mycursor = cnx.cursor()
        
        try: 
            mycursor.execute(query)
            cnx.commit() 

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)

    def check_comunidades(self):

        mydb = mysql.connector.connect(user='root',
                                    password=f"{self.contraseña}",
                                    host='127.0.0.1',
                                    database=f"{self.nombre_bbdd}",
                                    auth_plugin = f'{self.plugin}')
        mycursor = mydb.cursor()

        query_existe_comunidades = f"""
                SELECT DISTINCT comunidad FROM comunidades
                """
        mycursor.execute(query_existe_comunidades)
        comunidades = mycursor.fetchall()
        return comunidades
    
    def sacar_id_comunidad(self, comunidad):
        
        mydb = mysql.connector.connect(user='root',
                                       password= f'{self.contraseña}',
                                       host='127.0.0.1', 
                                       database=f"{self.nombre_bbdd}",
                                       auth_plugin = f'{self.plugin}')
        mycursor = mydb.cursor()
        
        try:
            query_sacar_id = f"SELECT idcomunidad FROM comunidades WHERE comunidad = '{comunidad}'"
            mycursor.execute(query_sacar_id)
            id_ = mycursor.fetchall()[0][0]
            return id_
        
        except: 
            return "Sorry, no tenemos esa ciudad en la BBDD y por lo tanto no te podemos dar su id. "
        
    def sacar_id_fecha(self, fecha):
        mydb = mysql.connector.connect(user='root', password=f'{self.contraseña}',
                                          host='127.0.0.1', database=f"{self.nombre_bbdd}",
                                          auth_plugin = f'{self.plugin}')
        mycursor = mydb.cursor()

        try:
            query_sacar_id = f"SELECT idfechas FROM fechas WHERE fecha = '{fecha}'"
            mycursor.execute(query_sacar_id)
            id_ = mycursor.fetchall()[0][0]
            return id_
        
        except: 
             return "Sorry, no tenemos esa fecha en la BBDD y por lo tanto no te podemos dar su id. "
         

