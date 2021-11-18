#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import numpy as np
import pandas as pd
from io import BytesIO
import PyPDF2
from bs4 import BeautifulSoup
from functools import reduce

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


##############################
###### Carne aviar############
##############################
url_aviar= 'https://www.magyp.gob.ar/sitio/areas/aves/estadistica/carne/index.php'
html_aviar = requests.get(url_aviar).text


# In[3]:


soup_aviar = BeautifulSoup(html_aviar, 'html.parser')
links_aviar = soup_aviar.find_all('a', href=True)


# In[4]:


#Loopeo para sacar el link
for link in links_aviar:
    if ('Indicadores Oferta y Demanda Carne Aviar' in link) and '.xls' in link.get('href'):
        aviar_xls = 'https://www.magyp.gob.ar/sitio/areas/aves/estadistica/carne/' + link.get('href')


# In[5]:


#Descargo el archivo, elimino columnas vacias y las filas que no tienen datos en la segunda columna
df_aviar = pd.read_excel(requests.utils.requote_uri(aviar_xls), engine='openpyxl')
df_aviar.dropna(how='all', axis=0, inplace=True)
df_aviar.dropna(subset=[df_aviar.columns[1]], inplace=True)


# In[6]:


#Completo los NaN en la primera fila y luego genero los nombres de las columnas
df_aviar.iloc[0].fillna(method='ffill', inplace=True)
df_aviar.columns = df_aviar.iloc[0, :] + ' - ' + df_aviar.iloc[1, :]
#Me deshago de las 2 primeras filas
df_aviar = df_aviar.iloc[2:, :]


# In[7]:


#Creo una columna con el año, completo los NaN
df_aviar['Year'] = pd.to_numeric(df_aviar[df_aviar.columns[0]], errors='coerce')
df_aviar['Year'].fillna(method='ffill', inplace=True)

#reemplazo los nombres de los meses en la columna original
#Se agrega la correccion para junio que figura en un solo año
dict_meses = {'Enero': 1, 'Febrero':2, 'Marzo':3, 'Abril':4, 'Mayo':5, 'junio':6, 'Junio':6, 'Julio':7, 'Agosto':8,
             'Septiembre':9, 'Octubre':10, 'Noviembre':11, 'Diciembre':12}
df_aviar[df_aviar.columns[0]].replace(dict_meses, regex=True, inplace=True)


# In[8]:


#Armo la columna de Date con año y mes
df_aviar['Date'] = df_aviar['Year'].astype(int).astype(str) + '-' + df_aviar[df_aviar.columns[0]].astype(str)

df_aviar['Date'] = pd.to_datetime(df_aviar['Date'], format='%Y-%m', errors='coerce')
df_aviar.dropna(subset=['Date'], inplace=True)


# In[9]:


#Elimino la primer columna y la penultima
df_aviar.drop(columns=[df_aviar.columns[0], df_aviar.columns[-2]], inplace=True)

df_aviar.set_index('Date', inplace=True)
#Agrego un prefijo para poder identificar de donde vienen los datos
df_aviar = df_aviar.add_prefix('Carne Aviar - ')
df_aviar.reset_index(inplace=True)


# In[10]:


##############################
###### Carne porcina##########
##############################
#Defino la url y descargo la pagina
url_porcino = 'https://www.magyp.gob.ar/sitio/areas/porcinos/estadistica/'

html_porcino = requests.get(url_porcino).text

soup_porcino = BeautifulSoup(html_porcino, 'html.parser')
links_porcino = soup_porcino.find_all('a', href=True)


# In[11]:


#Itero para encontrar el link
for link in links_porcino:
    ### A tener en cuenta, cambiaron la leyenda y le pusieron doble espacio
    if 'Evolucion Mensual  Indicadores' in link and '.xlsx' in link.get('href'):
        porcino_xls = 'https://www.magyp.gob.ar/sitio/areas/porcinos/estadistica/' + link.get('href')


# In[12]:


#Descargo el archivo
df_porcino = pd.read_excel(requests.utils.requote_uri(porcino_xls), engine='openpyxl', skiprows = 1)

#Se agrega una columna con el año en base a la primera columna
df_porcino['Year'] = pd.to_numeric(df_porcino[df_porcino.columns[0]], errors='coerce')
df_porcino['Year'].fillna(method='ffill', inplace=True)


# In[13]:


#En el diccionario de reemplazo de los nombres de los meses se agrega el NaN para eliminar la última fila
dict_mes = {'Var': np.nan,'Ene': 1, 'Feb':2, 'Mar':3, 'Abr':4, 'May':5, 'Jun':6, 'Jul':7, 'Ago':8,
             'Sep':9, 'Oct':10, 'Nov':11, 'Dic':12}
df_porcino[df_porcino.columns[0]].replace(dict_mes, regex=True, inplace=True)


# In[14]:


#Creo la columna de fecha en base al año y mes
df_porcino['Date'] = df_porcino['Year'].astype(int).astype(str) + '-' + df_porcino[df_porcino.columns[0]].astype(str)

df_porcino['Date'] = pd.to_datetime(df_porcino['Date'], format='%Y-%m', errors='coerce')
#Elimino las filas con NaN en las fechas
df_porcino.dropna(subset=['Date'], inplace=True)


# In[15]:


#Elimino la primera columna y la de año
df_porcino.drop(columns=[df_porcino.columns[0], df_porcino.columns[-2]], inplace=True)

df_porcino.set_index('Date', inplace=True)
df_porcino = df_porcino.add_prefix('Carne Porcina - ')
df_porcino.reset_index(inplace=True)


# In[16]:


##############################
###### Lecheria ##############
##############################

#Descargo la pagina
url_lecheria = 'https://www.magyp.gob.ar/sitio/areas/ss_lecheria/estadisticas/_01_primaria/index.php'

html_lecheria = requests.get(url_lecheria).text

soup_lecheria = BeautifulSoup(html_lecheria, 'html.parser')
links_lecheria = soup_lecheria.find_all('a', href=True)


# In[17]:


#Guardo los links con un archivo excel en una lista para despues quedarme con el primero
list_lecheria = []

for link in links_lecheria:
    if 'xls' in link.get('href'):
        list_lecheria.append(link.get('href'))


# In[18]:


#Descargo el primer elemento de la lista y elimino las columnas vacias
#Se trae el primer link que corresponde a la serie más actual (2015 en adelante)
df_lecheria = pd.read_excel(requests.utils.requote_uri(list_lecheria[0]), engine='openpyxl', skiprows = 6)
df_lecheria.dropna(how='all', axis=1, inplace=True)


# In[19]:


#Renombro las columnas
df_lecheria.rename(columns={df_lecheria.columns[0]:'Date', 
                            df_lecheria.columns[1]:'Producción Leche - Litros'}, inplace=True)
df_lecheria['Date'] = pd.to_datetime(df_lecheria['Date'], format = '%Y-%m-%d')

# df_lecheria.set_index('Date', inplace=True)


# In[20]:


###################################################################
###########Faena, Invernada e Indicadores bovinos##################
###################################################################

#Busco los links de los pdfs
url_base = 'https://www.magyp.gob.ar/sitio/areas/bovinos/informacion_sectorial/'
html_base = requests.get(url_base).text


# In[21]:


#Parseo y extraigo todos los links
soup = BeautifulSoup(html_base, 'html.parser')
links = soup.find_all('a', href=True)


# In[22]:


#Loopeo a través de todos los links y verifico que tengan el string de cada caso
string_faena = 'Tablero de faena bovina'
string_invernada = 'Tablero movimientos a invernada bovina'
string_ind_bovinos = 'Indicadores bovinos mensuales'

#Guardo los links en una lista
links_pdf = []

#El primer link corresponde a faena y el segundo a invernada
for link in links:
    if (string_faena in link) or (string_invernada in link) or (string_ind_bovinos in link.get_text()):
        links_pdf.append('https://www.magyp.gob.ar/sitio/areas/bovinos/informacion_sectorial/' + link.get('href'))


# In[23]:


#Descargo los pdfs
response_faena = requests.get(links_pdf[0])
response_invernada = requests.get(links_pdf[1])
response_ind_bovinos = requests.get(links_pdf[2])


# In[24]:


#Hago la lectura de los pdfs
pdf_faena = BytesIO(response_faena.content)
read_faena = PyPDF2.PdfFileReader(pdf_faena)

pdf_invernada = BytesIO(response_invernada.content)
read_invernada = PyPDF2.PdfFileReader(pdf_invernada)

pdf_ind_bovinos = BytesIO(response_ind_bovinos.content)
read_ind_bovinos = PyPDF2.PdfFileReader(pdf_ind_bovinos)


# In[25]:


# en el caso de indicadores bovinos, hay que loopear las paginas y encontrar la anotacion
number_pages = read_ind_bovinos.getNumPages()
for page in range(number_pages):
    page_keys = read_ind_bovinos.getPage(page).getObject()
    if '/Annots' in page_keys.keys():
        pagina_ind_bovinos = page


# In[26]:


#Cada archivo tiene 1 sola pagina, por lo que solo nos concentramos en obtener las anotaciones de esa
annots_faena = read_faena.getPage(0).getObject()['/Annots']
annots_invernada = read_invernada.getPage(0).getObject()['/Annots']
annots_ind_bovinos = read_ind_bovinos.getPage(pagina_ind_bovinos).getObject()['/Annots']


# In[27]:


set_faena = set()
set_invernada = set()
set_ind_bovinos = set()

for annot in annots_faena:
    set_faena.add(annot.getObject()['/A']['/URI'])
    
for annot in annots_invernada:
    set_invernada.add(annot.getObject()['/A']['/URI'])
    
for annot in annots_ind_bovinos:
    set_ind_bovinos.add(annot.getObject()['/A']['/URI'])


# In[28]:


########################################################
##########Hago las correcciones en el archivo de faena
########################################################
df_faena = pd.read_excel(requests.utils.requote_uri(list(set_faena)[0]), engine='openpyxl')


# In[29]:


#Elimino las columnas con NaN en todas sus filas
df_faena.dropna(how='all', axis=1, inplace=True)
#Completo las celdas vacias de la primera fila
df_faena.iloc[0, 1:].fillna(method='ffill', inplace=True)


# In[30]:


#Renombro las columnas
df_faena.columns = df_faena.iloc[0,:] + ' - ' + df_faena.iloc[1,:]

#Elimino las primeras filas y renombro la columna de fecha
df_faena = df_faena.iloc[2:, :]
df_faena.rename(columns={df_faena.columns[0]: 'Date'}, inplace=True)


# In[31]:


#Al convertir a datetime, lo que no es fecha queda como NaT y lo elimino al excluir los NaN para las filas
df_faena['Date'] = pd.to_datetime(df_faena['Date'], errors='coerce')
df_faena.dropna(how='all', axis=0, inplace=True)
df_faena.set_index('Date', inplace=True)

#Elimino las 4 primeras columnas que coinciden con el de indicadores bovinos
df_faena = df_faena.iloc[:, 4:]
df_faena = df_faena.add_prefix('Faena Bovina - ')
df_faena.reset_index(inplace=True)


# In[32]:


########################################################
#Hago las correcciones en el archivo de Invernada
########################################################
df_invernada = pd.read_excel(requests.utils.requote_uri(list(set_invernada)[0]), engine='openpyxl')


# In[33]:


#renombro las columnas
df_invernada.columns = df_invernada.iloc[2,:]

#Mantengo a partir de la tercer fila y reseteo el índice
df_invernada = df_invernada.iloc[3:, ]
df_invernada.reset_index(drop=True, inplace=True)


# In[34]:


#Determino cual seria la ultima fila y redimensiono el dataframe
final_row = df_invernada[df_invernada[df_invernada.columns[0]] == 'Total Año'].index[0]
df_invernada = df_invernada[:final_row]


# In[35]:


#Renombro la primera columna, cambio a datetime y la establezco como índice
df_invernada.rename(columns={df_invernada.columns[0]: 'Date'}, inplace=True)
df_invernada.rename_axis(None, axis=1, inplace=True)
df_invernada['Date'] = pd.to_datetime(df_invernada['Date'], errors='coerce')
df_invernada.set_index('Date', inplace=True)
df_invernada = df_invernada.add_prefix('Movimientos a Invernada - ')
df_invernada.reset_index(inplace=True)


# In[36]:


###########################
#Hago las correcciones en el archivo de Indicadores Bovinos
#Comparte informacion con faena (4 primeras columnas)
###########################
df_ind_bovinos = pd.read_excel(requests.utils.requote_uri(list(set_ind_bovinos)[0]), engine='openpyxl')


# In[37]:


#Elimino columnas vacias y completo los NaN de la primera fila
df_ind_bovinos.dropna(how='all', axis=1, inplace=True)
df_ind_bovinos.iloc[0].fillna(method='ffill', inplace=True)

#Cambio el nombre de las columnas y elimino las 2 primeras filas
df_ind_bovinos.columns = df_ind_bovinos.iloc[0, :] + ' - ' + df_ind_bovinos.iloc[1, :]
df_ind_bovinos = df_ind_bovinos.iloc[2:, :]


# In[38]:


#Renombro la primera columna y la seteo como indice
df_ind_bovinos.rename(columns={df_ind_bovinos.columns[0]:'Date'}, inplace=True)
df_ind_bovinos['Date'] = pd.to_datetime(df_ind_bovinos['Date'], errors='raise')
df_ind_bovinos.set_index('Date', inplace=True)

#Corrijo espacios y otros en el nombre de las columnas
df_ind_bovinos.columns = df_ind_bovinos.columns.str.replace('            ', ' ').str.replace('      ', ' ')
df_ind_bovinos.columns = df_ind_bovinos.columns.str.replace('  ', ' ').str.replace('\n', ' ').str.strip()

#Agrego el prefijo y reseteo el indice. Se resetea despues de agregar el 
df_ind_bovinos = df_ind_bovinos.add_prefix('Indicadores Bovinos - ')
df_ind_bovinos.reset_index(inplace=True)


# In[39]:


#Se crea una lista de dataframes
data_frames = [df_aviar, df_porcino, df_lecheria, df_faena, df_invernada, df_ind_bovinos]

#Se fusionan los dataframes de manera secuencial
df = reduce(lambda  left,right: pd.merge(left,right,on=['Date'], how='outer'), data_frames)

df.sort_values('Date', inplace=True)
df.set_index('Date', inplace=True)
df['country'] = 'Argentina'

alphacast.datasets.dataset(639).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
