#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm import tqdm

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


df = pd.read_csv("..//Input//Conector 84//NOOA Data.csv")
df = df.append(pd.read_csv("..//Input//Conector 84//NOOA Data 2.csv"))
df = df.append(pd.read_csv("..//Input//Conector 84//NOOA Data 3.csv"))
df = df.append(pd.read_csv("..//Input//Conector 84//NOOA Data 4.csv"))
df = df.append(pd.read_csv("..//Input//Conector 84//NOOA Data 5.csv"))
df["Date"] = pd.to_datetime(df["DATE"], errors= "coerce", format= "%Y-%m-%d")
del df["DATE"]
del df["LATITUDE"]
del df["LONGITUDE"]
del df["ELEVATION"]
del df["PRCP_ATTRIBUTES"]
del df["SNWD"]
del df["SNWD_ATTRIBUTES"]
del df["TAVG_ATTRIBUTES"]
del df["TMAX_ATTRIBUTES"]
del df["TMIN_ATTRIBUTES"]
del df["TMAX"]
del df["TMIN"]
df = df.drop_duplicates(keep="first", subset=["Date", "STATION"])
df = df.set_index(["Date", "STATION"])

def calc_running_sum(df_cut):
    df_cut = df_cut.sort_index()
    df_cut['PRCP'] = df_cut['PRCP'].fillna(0)
    df_cut["sum30"] = df_cut['PRCP'].rolling(30, min_periods = 30).sum().shift(-30)
    df_cut["sum180"] = df_cut['PRCP'].rolling(180, min_periods = 180).sum().shift(-180)
    df_cut["sum365"] = df_cut['PRCP'].rolling(365, min_periods = 365).sum().shift(-365)
    return df_cut

df_agg = pd.DataFrame()
for station in tqdm(df["NAME"].unique(), desc='Looping over different climate stations'):
    df_temp = calc_running_sum(df[df["NAME"]==station]) 
    df_agg = df_agg.append( df_temp)    

df_agg = df_agg.sort_index()    

df_agg = df_agg.rename(columns={"NAME": "country"})
df_agg = df_agg.reset_index()
df_agg["country"] = df_agg["country"] + " - " + df_agg["STATION"]
df_agg = df_agg.set_index(["Date", "STATION"])


df_obs = df_agg.groupby("country").count().sort_values("PRCP")
df_obs = df_obs[df_obs["PRCP"] > 10000]
df_agg = df_agg[df_agg["country"].isin(df_obs.index.unique())]
df_agg

df_agg = df_agg.reset_index().set_index("Date")

alphacast.datasets.dataset(84).upload_data_from_df(df_agg, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




