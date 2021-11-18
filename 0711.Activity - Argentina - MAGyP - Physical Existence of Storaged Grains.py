#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from functools import reduce

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[ ]:


#Esto sirve para mantener actualizados los datos

#Se descarga la pagina
page = requests.get('https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/'
                    'granos/_archivos/000058_Estad%C3%ADsticas/'
                    '000030_Existencia%20f%C3%ADsica%20de%20granos%20en%20plantas%20de%20almacenaje,'
                    '%20de%20servicios%20e%20industria.php')

#Se guardan todas las tablas en una lista
list_dfs = pd.read_html(page.content, thousands='.', decimal=',')

#Se crea una lista vacia donde se van a guardar todos los dataframes
data_frames = []

#Se itera por cada uno de las tablas
for i in range(2, len(list_dfs)):
    #Se asigna cada tabla a un dataframe temporal
    df_temp = list_dfs[i]
    dict_meses = {'ene':'01', 'feb':'02', 'mar':'03', 'abr':'04', 'may':'05', 'jun':'06', 'jul':'07', 'ago':'08',
                 'sep':'09', 'oct':'10', 'nov':'11', 'dic':'12'}
    #Se hace el reemplazo de los nombres de los meses
    df_temp['Mes'] = df_temp['Mes'].replace(dict_meses, regex=True)
    #Se convierte a formato fecha
    df_temp['Mes'] = pd.to_datetime(df_temp['Mes'].str[:2] + '-' + '20' + df_temp['Mes'].str[-2:] , format='%m-%Y')
    #Se renombra la columna
    df_temp.rename(columns={'Mes':'Date'}, inplace=True)
    #Se establece la columna de fecha como indice
    df_temp.set_index('Date', inplace=True)
    #Se guarda en la lista de dataframes procesados
    data_frames.append(df_temp)

#Se fusionan los dataframes en uno final
df = reduce(lambda left,right: pd.merge(left,right,left_index=True, right_index=True, 
                                        how='outer'), data_frames)
#Como a partir de 2018 no hay 
if 'Cebada' not in df.columns:
    df['Cebada'] = df[['Ceb. Cervecera', 'Ceb. Forrajera','Ceb. apta para Malteria']].sum(axis=1)

df['country'] = 'Argentina'


# In[ ]:


alphacast.datasets.dataset(7967).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)




