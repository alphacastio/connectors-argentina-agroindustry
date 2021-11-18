#!/usr/bin/env python
# coding: utf-8


import requests
import pandas as pd
from bs4 import BeautifulSoup


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


page = requests.get('http://www.ciaracec.com.ar/ciara/Comunicaci%c3%b3n/Divisas')
soup = BeautifulSoup(page.content, 'html.parser')

link_xls = []
for link in soup.find_all('a'):
    if link.get('href') and 'xls' in link.get('href') and 'liquidacion' in link.get('href'):
        link_xls.append('http://www.ciaracec.com.ar' + link.get('href'))

response = requests.get(link_xls[0])


df = pd.read_excel(link_xls[0], sheet_name = "Datos Mensuales", skiprows=2, engine='xlrd')
df = df.iloc[:15]



#Renombro las columnas
columnas = [df.iloc[0, i] if pd.isna(df.iloc[1, i]) else df.iloc[1, i] for i in range(1, df.shape[1])]
df.columns = ['Mes'] + columnas



#Elimino las 3 primeras filas y stackeo
df = df.iloc[3:]
df = df.set_index('Mes').stack().reset_index()
df['Mes'] = df['Mes'].str.strip()


#Reemplazo los nombres de los meses a
dict_meses = {'Enero':'01', 'Febrero':'02', 'Marzo':'03', 'Abril':'04', 'Mayo':'05', 'Junio':'06',
              'Julio':'07', 'Agosto':'08', 'Septiembre':'09', 'Octubre':'10', 'Noviembre':'11', 'Diciembre':'12'}

df['Mes'] = df['Mes'].replace(dict_meses)



#Creo la columna Date y la paso a formato fecha
df['Date'] = pd.to_datetime(df['level_1'].astype(int).astype(str) + '-' + df['Mes'], format='%Y-%m', errors='coerce')



df.rename(columns={0:'Liquidacion divisas'}, inplace=True)
df = df[['Date', 'Liquidacion divisas']]


df.set_index('Date', inplace=True)
df['country'] = 'Argentina'
df.sort_index(inplace=True)



alphacast.datasets.dataset(327).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



