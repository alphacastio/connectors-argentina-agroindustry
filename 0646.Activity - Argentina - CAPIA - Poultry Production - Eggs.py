#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from bs4 import BeautifulSoup
import io
import tabula

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


#Descargamos la pagina para buscar el link del pdf
page = requests.get('https://www.magyp.gob.ar/sitio/areas/aves/estadistica/huevos/index.php')


# In[3]:


soup = BeautifulSoup(page.content, 'html.parser')


# In[4]:


#Buscamos el link que tiene en el texto visible la leyenda de Produccion de Huevos Industrializado
for link in soup.find_all('a'):
    if 'Producción de Huevos Industrializado' in link.get_text():
        pdf_link = 'https://www.magyp.gob.ar/sitio/areas/aves/estadistica/huevos/' + link.get('href')


# In[5]:


#Descargo el archivo pdf
response_pdf = requests.get(pdf_link)


# In[6]:


#Leo con tabula y lo paso a un dataframe
#como genera una lista, asigno el primer objeto
df = tabula.read_pdf(io.BytesIO(response_pdf.content), stream=True, pages = 1)[0]


# In[7]:


#Elimino la columna intermedia entre los años y la ultima con la variacion interanual
df.drop([df.columns[2], df.columns[4]], axis=1, inplace=True)


# In[8]:


#Hago un stack para tener el mes y año
df = df.set_index(df.columns[0]).stack().reset_index()


# In[9]:


#Hago el reemplazo de los nombres de los meses por su número correspondiente
dict_meses = {'Enero':'01', 'Febrero':'02', 'Marzo':'03', 'Abril':'04', 'Mayo':'05', 'Junio':'06', 'Julio':'07',
       'Agosto':'08', 'Septiembre':'09', 'Octubre':'10', 'Noviembre':'11', 'Diciembre':'12'}

df['Año'] = df['Año'].replace(dict_meses, regex=True)


# In[10]:


#Convierto a formato fecha
df['Date'] = pd.to_datetime(df['level_1'].astype(str) + '-' + df['Año'], format='%Y-%m')


# In[11]:


#Mantengo solamente las columnas de fecha y produccion y luego ordeno por el indice
df = df[['Date', 0]]
df.set_index('Date', inplace=True)
df.sort_index(inplace=True)


# In[12]:


#Renombro la columna y multiplico por 1 millon para que quede en la misma escala de la informacion de Capia
df.columns = ['Producción Avícola - Huevos']
df = df*1000*1000


# In[13]:


df['country'] = 'Argentina'



alphacast.datasets.dataset(646).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




