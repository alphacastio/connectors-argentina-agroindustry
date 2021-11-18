#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
import urllib
import time
from urllib.request import urlopen
import requests  

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)
#Preguntar a mike multi entity y ver si se puede pisar el nro de dataset desde conectores
#Dataset name: Agriculture - Argentina - Bolsa de Cereales - Panorama Agricola Semanal (PAS)
#Dataset id: 5680


# In[2]:


url = "https://www.bolsadecereales.com/admin/phpexcel/Examples/reporte_bd.php?cultivo=&campania=&zona=&t=PAS"
r = requests.get(url, verify = False)
df = pd.read_excel(r.content)
df['Date'] = df["Campaña"].str[:4] + "-01-01"
df.index = pd.to_datetime(df['Date'])
df.index.rename('Date', inplace = True) 
del df['Date']

alphacast.datasets.dataset(696).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




