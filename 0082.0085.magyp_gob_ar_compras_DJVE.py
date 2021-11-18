#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from lxml import etree
from tqdm import tqdm


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

import datetime
dateparser = 60
days= []
today = datetime.date.today()
initday = today

amountdays = dateparser+1   
for i in tqdm(range(0, amountdays), desc='Looping over different days'):
    properday = initday - datetime.timedelta(days=i)
    if properday != today:

        if properday.weekday() > 4:
            pass
        else:
            year = properday.year
            weekday = properday.weekday()
            if properday.weekday() <= 4:
                date= properday.strftime('%Y-%m-%d')
                days.append(date)


def parse_date(response_data):
    html = response_data.content
    htmlparser = etree.HTMLParser()
    tree = etree.fromstring(html, htmlparser)
    cultivos = tree.xpath(".//*[@id='TabbedPanels1']/ul/li[@class='TabbedPanelsTab']/text()")    
    df_cultivo = pd.DataFrame(cultivos, columns=['cultivo'])

    magyp = pd.DataFrame([])
    df_value_array = pd.DataFrame()
    sectors_array = []
    campana_array = []
    table_tags = tree.xpath(".//table[@class='tabla']")

    for k,table_tag in enumerate(table_tags):
        date_str_array = days
        if date_str_array:
            date_str = days[z]

        columns = []
        sectors = []
        campana = []
        final_values = []

        tr_tags = table_tag.xpath(".//tr")
        for i, tr_tag in enumerate(tr_tags):
            if i == 0:
                th_headers = tr_tag.xpath('./th//text()')
                for col in th_headers[1:]:
                    
                    if "\r\n" in col:
                        continue
                    columns.append(col.split(" (")[0])
                continue
                
            th_data = tr_tag.xpath('./th[1]//text()')
            if th_data:
                sectors_array.append(' '.join(list(x.strip() for x in th_data)))

            values = []
            td_datas = tr_tag.xpath('./td/text() | ./td/strong/text()')
            longstring = ''.join(td_datas[1:]).replace("(*)", "").replace("(**)", "").replace("(***)", "")
            if ('(' in longstring) and (')' in longstring):                
                continue
            
            
            for j, td_data in enumerate(td_datas):
                td_data = td_data.replace("(*)", "").replace("(**)", "").replace("(***)", "").strip()
                if ('/' in td_data):
                    count = 0
                    #print("{}:{}".format(count, td_data))
                    fixed_data = td_data.replace(',', '.')
                    if len(fixed_data) - len(fixed_data.replace('.', '')) == 2:
                        a, b, c = fixed_data.partition('.')
                        fixed_data = a + b.replace('.', '') + c
                    
                    values.append(fixed_data)
                    campana.append(td_data)
                elif '(' in td_data or count >= len(columns)-1:
                    continue
                elif td_data == "":
                    continue                    
                else:
                    count += 1
                    fixed_data = td_data.replace(',', '.')
                    if len(fixed_data) - len(fixed_data.replace('.', '')) == 2:
                        a, b, c = fixed_data.partition('.')
                        fixed_data = a + b.replace('.', '') + c                    
                    values.append(fixed_data)


            if values:
                final_values.append(values)
                
        if final_values:
            df_values = pd.DataFrame(final_values)
            df_values.columns = columns

        if k != 7:
            df_values['Producto'] = df_cultivo.cultivo[k]

        df_value_array = df_value_array.append(df_values)
        if campana:
            campana_array.append(campana)

        df_sectors_array = pd.DataFrame(sectors_array)[0].repeat(2)
        df_value_array['Sector'] = df_sectors_array.reset_index()[0] 
        df_value_array['Date'] = days[z]
        
    return df_value_array


# In[14]:


magyp = pd.DataFrame([])
         
for z, properday in tqdm(enumerate(days), desc='Looping for getting the data'):
    timedate = pd.to_datetime(properday, format='%Y-%m-%d')
    year = timedate.year
    weekday = timedate.weekday()
    data_url = "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/areas/granos/_archivos/000058_Estad%C3%ADsticas/_compras_historicos/{}/01_embarque_{}.php".format(year,properday)

    response_data = requests.get(data_url)

    if response_data.status_code == 200:
        magyp = magyp.append(parse_date(response_data))




temp = magyp.copy
for field in ['Semanal', 'Total Comprado', 'Total Precio Hecho', 'Total a Fijar', 'Total Fijado', 'Saldo a Fijar', 'DJVE',
       'Total Acumulado']:
    try:
        magyp[field] = pd.to_numeric(magyp[field], errors="coerce")
    except:
        continue
magyp["Sector"] = magyp["Sector"].str.split(" \(").str[0]    


magyp = magyp.reset_index()
magyp["country"] = magyp["Producto"] + " - " + magyp["Cosecha"] + " - " + magyp["Sector"]
magyp["Date"] = pd.to_datetime(magyp["Date"], format = "%Y-%m-%d").dt.date
magyp = magyp.set_index("Date")


magyp = magyp.drop_duplicates()
alphacast.datasets.dataset(82).upload_data_from_df(magyp, deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)
alphacast.datasets.dataset(85).upload_data_from_df(magyp, deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)