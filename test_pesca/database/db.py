import pandas as pd
import numpy as np
import sqlite3

#importo csv e creo dataframe

df_andamento=pd.read_csv('csv/andamento.csv', sep=';', encoding='latin1')
df_importanza=pd.read_csv('csv/importanza.csv', sep=';', encoding='latin1')
df_produttvita=pd.read_csv('csv/produttivita.csv', sep=';', encoding='latin1')

#creo connessione al db
db = sqlite3.connect('database/db.sqlite')
cursor = db.cursor()
db.commit()

#creo tabelle
df_importanza.to_sql('importanza', db, if_exists='replace', index=False)
db.commit()

df_andamento.to_sql('andamento', db, if_exists='replace', index=False)
db.commit()

df_produttvita.to_sql('produttivita', db, if_exists='replace', index=False)
db.commit()

#creo df per ogni tabella con eventuali valori nulli

df_prod_null=df_produttvita[df_produttvita.isnull().any(axis=1)]
df_and_null=df_andamento[df_andamento.isnull().any(axis=1)]
df_imp_null=df_importanza[df_importanza.isnull().any(axis=1)]


#inserisco valori medi al posto dei dati null per la regione valle d'aosta, ho scelto di utilizzare la media dei valori precedenti

df_aosta_prod=df_produttvita[df_produttvita['Regione']== "Valle d'Aosta"]
media_prod_aosta=np.mean(df_aosta_prod['Produttività in migliaia di euro'])

df_aosta_and=df_andamento[df_andamento['Regione']== "Valle d'Aosta"]
media_and_aosta=np.mean(df_aosta_and['Variazione percentuale unità di lavoro della pesca'])

print(media_prod_aosta)
print(media_and_aosta)

df_andamento.fillna({"Variazione percentuale unità di lavoro della pesca":media_and_aosta}, inplace=True)
df_produttvita.fillna({"Produttività in migliaia di euro":media_prod_aosta}, inplace=True)

#sostituisco le vecchie tabelle con le nuove contenenti i valori indicati
df_importanza.to_sql('importanza', db, if_exists='replace', index=False)
db.commit()

df_andamento.to_sql('andamento', db, if_exists='replace', index=False)
db.commit()

df_produttvita.to_sql('produttivita', db, if_exists='replace', index=False)
db.commit()

#serie calcolate

#creo un dataframe per ogni zona d'italia per la produttività
nord_ovest=["Valle d'Aosta", "Piemonte", "Liguria", "Lombardia"]
nord_est=["Trentino-Alto Adige", "Veneto", "Friuli-Venezia Giulia", "Emilia-Romagna"]
centro=["Toscana", "Umbria", "Lazio", "Marche", "Abruzzo"]
sud=["Molise", "Puglia", "Campania", "Basilicata", "Calabria"]
isole=["Sicilia", "Sardegna"]

aree=[
    ["Valle d'Aosta", "Piemonte", "Liguria", "Lombardia"],
    ["Trentino-Alto Adige", "Veneto", "Friuli-Venezia Giulia", "Emilia-Romagna"],
    ["Toscana", "Umbria", "Lazio", "Marche", "Abruzzo"],
    ["Molise", "Puglia", "Campania", "Basilicata", "Calabria"],
    ["Sicilia", "Sardegna"]
    ]   

aree_etichetta=[
    'Nord-Ovest',
    'Nord-Est',
    'Centro',
    'Sud',
    'Isole'
]

#a partire da un dataframe resituisce una lista di df divisi per area
def split_df_by_area(df):
    df_x_area=[]
    for a in aree:
        df_area=df[df['Regione'].isin(a)]
        df_x_area.append(df_area)
    return df_x_area


aree_prod=split_df_by_area(df_produttvita)
aree_imp=split_df_by_area(df_importanza)
aree_and=split_df_by_area(df_andamento)



df_serie_calcolate=pd.DataFrame(columns=["Anno", "Serie calcolata", "Valore"])

#punto 1 serie calcolate
def somma_aree(stringa):
    anni=df_produttvita["Anno"].unique()
    for a in anni:
        for x, y in zip(aree_prod, aree_etichetta) :
            x=x[x['Anno']==a]
            somma_prod=x["Produttività in migliaia di euro"].sum()
            df_serie_calcolate.loc[len(df_serie_calcolate.index)] = [a, stringa + ' ' + y, somma_prod] 

somma_aree("Somma produttività")
df_serie_calcolate.to_sql('serie_calcolate', db, if_exists='replace', index=False)

# #punto 2 serie calcolate
anni=df_produttvita["Anno"].unique()
for a in anni:
    df_prod_naz=df_produttvita[df_produttvita["Anno"]== a]
    tot_nazionale_prod=df_prod_naz["Produttività in migliaia di euro"].sum()
    df_serie_calcolate.loc[len(df_serie_calcolate.index)] = [a, "Totale produttività NAZIONALE", tot_nazionale_prod] 

df_serie_calcolate.to_sql('serie_calcolate', db, if_exists='replace', index=False)

#punto 3 serie calcolate
def media_aree(stringa):
    anni=df_importanza["Anno"].unique()
    for a in anni:
        for x, y in zip(aree_imp, aree_etichetta) :
            x=x[x['Anno']==a]
            media=np.mean(x["Percentuale valore aggiunto pesca-piscicoltura-servizi"])
            df_serie_calcolate.loc[len(df_serie_calcolate.index)] = [a, stringa + ' ' + y, media]

media_aree("Media percentuale valore aggiunto")

df_serie_calcolate.to_sql('serie_calcolate', db, if_exists='replace', index=False)

#punto 4 serie calcolate
anni_and=df_andamento["Anno"].unique()
for a in anni_and:
    df_media_aree=df_andamento[df_andamento["Anno"]==a]
    media_nazionale_var=np.mean(df_media_aree["Variazione percentuale unità di lavoro della pesca"])
    df_serie_calcolate.loc[len(df_serie_calcolate.index)] = [a, "Media NAZIONALE variazione percentuale occupazione", media_nazionale_var] 

df_serie_calcolate.to_sql('serie_calcolate', db, if_exists='replace', index=False)

#punto 5 serie calcolate
def media_variazione_aree(stringa):
    anni=df_andamento["Anno"].unique()
    for a in anni:
        for x, y in zip(aree_and, aree_etichetta) :
            x=x[x["Anno"]==a]
            media=np.mean(x["Variazione percentuale unità di lavoro della pesca"])
            df_serie_calcolate.loc[len(df_serie_calcolate.index)] = [a, stringa + ' ' + y, media]

media_variazione_aree("Media variazione percentuale occupazione")
df_serie_calcolate.to_sql('serie_calcolate', db, if_exists='replace', index=False)



