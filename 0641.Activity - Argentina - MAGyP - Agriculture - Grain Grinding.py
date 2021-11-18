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
    if 'Evolución de la Molienda Mensual - Oleaginosas' in link.get('href'):
        lista_anos.append(url + link.get('href'))

#Hago un reverse para que ordene bien los años
lista_anos.reverse()


# In[6]:


#Defino una funcion para aplicar a cada año
def correccion_tabla(url):
    #Descarga de la info
    df0 = pd.read_html(requests.utils.requote_uri(url), thousands='.', decimal=',')[-1]
    
    #Si es multi index
    if df0.columns.nlevels == 2:    
        #Paso del multi-index a uno flatten
        df0.columns = [' - '.join(x) for x in df0.columns]
    else:
        #Si no es multi index, cambio el nombre de las columnas y elimino las 3 primeras filas
        df0.columns  = df0.iloc[1] + ' - ' + df0.iloc[2]
        df0 = df0.iloc[3: , :]
        df0.reset_index(drop=True, inplace=True)
        
    #Hago un reemplazo en los nombres
    df0.columns= df0.columns.str.replace('G R A N O S O L E A G I N O S O S' , 
                                         'GRANOS OLEAGINOSOS').str.replace('A C E I T E S', 'ACEITES')
    #Identifico en que fila se encuentra el otro titulo
    key_row = df0[df0[df0.columns[0]]=='P E L L E T S'].index[0]
    
    #Copio el dataframe original a uno nuevo y lo parto en 2
    df1 = df0.copy()
    df0 = df0.iloc[:key_row]
    df1 = df1.iloc[key_row:]
    #Hago una limpieza de los nombres de las columnas del segundo dataframe
    df1.columns = df1.iloc[0] + ' - ' + df1.iloc[1]
    df1.columns = df1.columns.str.replace('P E L L E T S', 'PELLETS').str.replace('E X P E L L E R S', 'EXPELLERS')
    df1 = df1.iloc[2:,:]
    
    #Defino un diccionario para hacer reemplazos de los nombres de los meses
    dict_meses = {'ENER0':1, 'ENERO':1, 'FEBRERO':2, 'MARZO':3, 'ABRIL':4, 'MAYO':5, 'JUNIO':6, 'JULIO':7, 'AGOSTO':8,
              'SEPTIEMBRE':9, 'OCTUBRE':10, 'NOVIEMBRE':11, 'DICIEMBRE':12}
    
    #Creo una lista vacia donde se van a colocar los 2 dataframes
    df_list = []
    
    for dframe in [df0, df1]:
        #Creo una nueva columna donde extraigo el año de la primera columna
        dframe['Year'] = pd.to_numeric(dframe[dframe.columns[0]], errors='coerce')
        #Hago un reemplazo de los nombres de los meses por su numero de mes
        dframe[dframe.columns[0]].replace(dict_meses, regex=True, inplace=True)
        #Completo la columna de años
        dframe['Year'].fillna(method='ffill', inplace=True)
        #Creo la columna de fechas a partir de los años y meses
        dframe['Date'] = dframe['Year'].astype(int).astype(str) + '-' + dframe[dframe.columns[0]].astype(str)
        dframe['Date'] = pd.to_datetime(dframe['Date'], format='%Y-%m', errors='coerce')
        dframe.dropna(subset=['Date'], inplace=True)
        #Elimino las columnas de meses y año
        dframe.drop(columns=[dframe.columns[0], dframe.columns[-2]], inplace=True)
        df_list.append(dframe)
    
    #Hago el merge de los 2 dataframes del año
    df = df_list[0].merge(df_list[1], left_on='Date', right_on='Date', how='outer')
    df.set_index('Date', inplace=True)
    
    return df


# In[7]:


#Itero sobre cada url, lo paso a un dataframe temporal y los voy agregando al definitivo
for indice, ano in enumerate(lista_anos):
    df_temp=correccion_tabla(ano)
    if indice == 0:
        df = df_temp.copy()
    else:
        df = df.append(df_temp)

#Agrego la entidad
df['country'] = 'Argentina'




alphacast.datasets.dataset(641).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
