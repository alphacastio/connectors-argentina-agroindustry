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


#########################################################
###Esto trae todo el historico de circulares hasta la fecha
#########################################################

# post_data = '{"MPage":false,"cmpCtx":"", "parms":["27","0",9,1,false,"    /  /   00:00:00","    /  /   00:00:00",0,0,0,0,"GRID_DIM_PrecioFob_Fecha","GRID_DIM_PrecioFob_NCirc","GRID_DIM_PrecioFob_NComu","DINEM_FOB.WP_FOB_ConsAll","2021/09/06 00:00:00",840,166,1,"3"],"hsh":[],"objClass":"dinem_fob.wp_fob_consall","pkgName":"GeneXus.Programs","events":["GRIDPAGINATIONBAR.CHANGEPAGE"],"grids":{"Grid":{"id":49,"lastRow":9}}}'
# page = requests.post('https://dinem.agroindustria.gob.ar/dinem_fob.wp_fob_consall.aspx', data=post_data)

# soup = BeautifulSoup(page.content, 'html.parser')
# input_table = soup.find('input', {'name': 'GXState'})

#Desde 1993 se publican las circulares, son 7067 (hasta el 17/09)
# list_circulares = []
# for i in range(0,7068):
#     posicion = str(i)
#     list_circulares.append(eval(eval(input_table.get('value'))['GridContainerData'])[posicion]['Props'][0][1])


# In[ ]:


###Funcion a la que se le pasa la url y descarga la tabla con los datos

def table_fob(url_circular):
    #Descargo la pagina para cada fecha
    page_fecha = requests.get('https://dinem.agroindustria.gob.ar/' + url_circular)

    #Extraigo la fecha de la url para agregarla al dataframe
    fecha = url_circular.replace('dinem_fob.wp_fob_conslista.aspx?','')
    fecha = fecha.split(',')[0]
    fecha = pd.to_datetime(fecha, format='%Y%m%d')

    #Parseo la pagina y extraigo la tabla con valores
    soup_fecha = BeautifulSoup(page_fecha.content, 'html.parser')
    input_table_fecha = soup_fecha.find('input', {'name':"GridContainerDataV"})


    df_fecha = pd.DataFrame(eval(input_table_fecha.get('value')), 
                            columns=['Producto', 'Descripción', 'Precio (Dls/Ton)', 'Emb. Desde', 'Emb. Hasta'])
    
    #Se agrega el numero de fila para que se pueda ordenar cuando se descarga
    orden = range(1, len(df_fecha) + 1)
    df_fecha.insert(0, 'orden', orden)

    #Inserto la fecha en la primera columna
    df_fecha.insert(0, 'Date', fecha)

    df_fecha.set_index('Date', inplace=True)
    
    return df_fecha


# In[ ]:


###Se descarga el listado de las ultimas 9 circulares de https://dinem.agroindustria.gob.ar/dinem_fob.wp_fob_consall.aspx
page = requests.get('https://dinem.agroindustria.gob.ar/dinem_fob.wp_fob_consall.aspx')

#Se extraen los links
soup = BeautifulSoup(page.content, 'html.parser')
input_table = soup.find('input', {'name': 'GXState'})
tabla = eval(input_table.get('value'))

circulares = []

#Se agrega lo de true y false porque en los diccionarios figura pero python no lo reconoce
true=True
false=False

#Se iteran por las paginas (definir funcion), agregar una columna de fecha
for i in range(0,9):
    posicion = str(i)
    circulares.append(eval(tabla['GridContainerData'])[posicion]['Props'][0][1])

#Se revierte el orden de la lista para que comience a iterar por la fecha mas antigua
circulares.reverse()


# In[ ]:


for indice, circular in enumerate(circulares):
    df_temp = table_fob(circular)
    if indice==0:
        df=df_temp.copy()
    else:
        df = df.append(df_temp)

#Para que la API no elimine las filas, se reemplaza los vacios por el guion medio        

df['Emb. Desde'] = df['Emb. Desde'].replace('','-')
df['Emb. Hasta'] = df['Emb. Hasta'].replace('','-')
df['country'] = 'Argentina'



alphacast.datasets.dataset(7689).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)
