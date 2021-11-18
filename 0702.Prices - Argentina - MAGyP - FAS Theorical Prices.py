#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[ ]:


#Se descargo el historico y para las fechas siguientes, se hace un append
#de los datos que aparecen en la tabla de la pagina

url = 'https://dinem.agroindustria.gob.ar/dinem_fas.cfasn.aspx'


# In[ ]:


#Se descarga la página y se la parsea
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')


# In[ ]:


#se busca la tabla que contiene los valores
input_data = soup.find('input', {'name':'GridContainerDataV'})


# In[ ]:


#Se convierte a dataframe y se renombra las columnas
df = pd.DataFrame(eval(input_data.get('value')), 
        columns=['Date', 'Trigo Pan', 'Maíz', 'Girasol', 'Soja', 'Aceite crudo de Girasol', 'Aceite crudo de Soja'])


# In[ ]:


#Se establece el indice y se agrega el country
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
df.set_index('Date', inplace=True)
df['country'] = 'Argentina'
df.sort_index(inplace=True)


# In[ ]:


#Al fijar el deleteMissingFromDB=False, mantiene los valores que no aparecen en los datos nuevos y agrega
#los nuevos


alphacast.datasets.dataset(7687).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




