#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


data_url_to_get_files = "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/djve/_archivos/000011_Declaraciones%20Juradas%20de%20Ventas%20al%20Exterior%20(Ley%2021.453).php"
page_html = requests.get(data_url_to_get_files).text


# In[3]:


soup = BeautifulSoup(page_html, "html.parser")


# In[4]:


#Extraigo todos los links
links = soup.find_all('a', href=True)

#Extraigo todos los valores del menu desplegable con los archivos para cada año
items = soup.select('option[value]')
values = [item.get('value') for item in items]
#Se revierte el orden de la lista para que comience con los de 2011
values.reverse()


# In[5]:


#Hago un loop para extraer el link del archivo de las DJVE acumuladas actual
for link in links:
    if 'DJVE Granos Acumuladas Aprobadas' in link.get_text():
        values.append(link.get('href'))


# In[6]:


#Hago un loop sobre la lista de urls, leo los archivos y elimino columnas
for indice, value in enumerate(values):
    df_temp = pd.read_excel(requests.utils.requote_uri(value), engine='openpyxl')
    #Mantengo solamente unas columnas
    df_temp = df_temp[['FECHA DE \nREGISTRO', 'PRODUCTO', 'TONS', 'OPCION']]
    df_temp.rename(columns={df_temp.columns[0]: 'Date', df_temp.columns[2]: "Value"}, inplace=True)
    if indice==0:
        df = df_temp.copy()
    else:
        df = df.append(df_temp)


# In[7]:


# Hago el replace de las tildes en los nombres de los productos
dict_tildes = {'Á': 'A', 'É':'E', 'Í':'I', 'Ó':'O','Ú':'U'}
df['PRODUCTO'].replace(dict_tildes, regex=True, inplace=True)
df['PRODUCTO'] = df['PRODUCTO'].str.strip()


# In[8]:


###########################################################################
#########################  Conector 100    ################################
###########################################################################
df_100 = df.copy()
#Agrego como entidad al producto-opcion
df_100['country'] = df_100['PRODUCTO']+'-'+ df_100['OPCION'].astype(str)


# In[9]:


#Pivoteo para que haga la suma de los valores
df_100 = df_100.pivot_table(index=['Date', 'country'], values='Value', aggfunc= np.sum).reset_index()
df_100.rename(columns = {'Value': 'Total TONs'}, inplace=True)
df_100.set_index('Date', inplace=True)


alphacast.datasets.dataset(100).upload_data_from_df(df_100, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[11]:


###########################################################################
#########################  Conector 101    ################################
###########################################################################
df_101 = df.copy()
df_101.drop('OPCION', axis=1, inplace=True)


# In[12]:


#Se pivotea para que cada producto quede en una columna y que la frecuencia sea diaria
df_101 = df_101.pivot_table(index='Date', columns='PRODUCTO', values='Value', aggfunc=np.sum).reset_index()
df_101.rename_axis(None, axis=1, inplace=True)
df_101.set_index('Date', inplace=True)
df_101['country'] = 'Argentina'



alphacast.datasets.dataset(101).upload_data_from_df(df_101, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[14]:


###########################################################################
#########################  Conector 102    ################################
###########################################################################
df_102 = df.copy()
df_102.drop('OPCION', axis=1, inplace=True)


# In[15]:


#Se agrupa por mes y cada columna corresponde a un producto
#Se pasa a formato de mes y luego a datetime nuevamente
df_102['Date'] = pd.to_datetime(df_102['Date'].dt.strftime('%Y-%m-01'), format = ('%Y-%m-%d'))


# In[16]:


#El pivoteo hace que la frecuencia sea mensual
df_102 = df_102.pivot_table(index='Date', columns='PRODUCTO', values='Value', aggfunc=np.sum).reset_index()
df_102.rename_axis(None, axis=1, inplace=True)
df_102.set_index('Date', inplace=True)
df_102['country'] = 'Argentina'



alphacast.datasets.dataset(102).upload_data_from_df(df_102, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




