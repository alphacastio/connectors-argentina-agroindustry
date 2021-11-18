#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url = 'https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/granos/'


# In[3]:


page_html = requests.get(url).text
soup = BeautifulSoup(page_html, 'html.parser')


# In[4]:


links = soup.find_all('a', href=True)


# In[5]:


#Hago un loop para extraer los links
lista_anos = []
for link in links:
    if 'Evolución de la Molienda Mensual de Cereales' in link.get('href'):
        lista_anos.append(url + link.get('href'))

#Hago un reverse para que ordene bien los años
lista_anos.reverse()


# In[6]:


def tabla_magyp(ano):
    df = pd.read_html(requests.utils.requote_uri(ano), thousands = '.', decimal = ',')[-1]
    
    #En el primer año la tabla tiene una fila adicional, por lo que verificamos cual es el primer valor
    #Si trae la leyenda de dirección de mercados, se debe saltear 1 fila adicional
    if df.iloc[0,0] == 'DIRECCIÓN DE MERCADOS AGRÍCOLAS':
        df = df.iloc[2:, :]
    else:
        df = df.iloc[1:, :]
    
    #Eliminamos filas y columnas con todos valores NaN
    df.dropna(how='all', axis=0, inplace =True)
    df.dropna(how='all', axis=1, inplace =True)
    #Como eliminamos filas, conviene resetear el indice
    df.reset_index(drop=True, inplace=True)
    
    #Creamos una lista para recopilar los nombres de las columnas
    #Si el valor de la primera fila coincide con el de la tercera, entonces agregamos el de la primera fila
    #De lo contrario, juntamos la leyenda de la primera fila y de la tercera
    columnas = []
    for i in range(df.shape[1]):
        if df.iloc[0, i] == df.iloc[2, i]:
            columnas.append(df.iloc[0, i])
        else:
            columnas.append(df.iloc[0, i] + ' - ' + df.iloc[2, i])
    
    #Reemplazamos el nombre de las columnas
    df.columns =  columnas
    df.columns =  df.columns.str.replace('  ', ' ').str.replace('BALAN.y', 'BALAN. y')
    df.columns =  df.columns.str.replace('^SORGO$', 'SORGO - TOTAL', regex=True)
    
    #Eliminamos las 3 primeras filas y reseteamos el índice
    df = df.iloc[3:, :]
    df.reset_index(drop=True, inplace=True)
    
    #Guardamos el año en una variable. Traemos los primeros 4 caracteres de la celda primera celda de la tabla
    year = df.iloc[0,0][:4]
    
    #Renombramos la primera columna que tiene la fecha
    df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
    
    #Mantengo las filas que no tienen los mismos valores para todas las columnas
    df = df[df.nunique(1)>1]
    
    #Creamos un diccionario y hacemos los reemplazos de los nombres
    dict_meses = {'ENERO': '01', 'FEBRERO':'02', 'MARZO':'03', 'ABRIL':'04', 'MAYO':'05', 'JUNIO':'06',
                 'JULIO': '07', 'AGOSTO': '08', 'SEPTIEMBRE':'09', 'OCTUBRE':'10', 'NOVIEMBRE':'11', 'DICIEMBRE':'12'}
    df['Date'].replace(dict_meses, inplace=True)
    
    #Construimos la fecha al concatenar el año y el mes
    df['Date'] = year +'-' + df['Date']
    #Lo pasamos a formato fecha
    df['Date'] = pd.to_datetime(df['Date'], format = '%Y-%m', errors='coerce')
    
    #Eliminamos las filas que tienen NaN en la columna Date y devolvemos el dataframe
    df.dropna(subset=['Date'], inplace=True)
    
    return df


# In[7]:


for indice, ano in enumerate(lista_anos):
    df_temp = tabla_magyp(ano)
    
    if indice==0:
        df = df_temp.copy()
    else:
        df = df.append(df_temp, ignore_index=True)


# In[8]:


df.set_index('Date', inplace=True)
df['country'] = 'Argentina'



alphacast.datasets.dataset(660).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




