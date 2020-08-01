import plotly as py
import plotly.graph_objs as go
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import numpy as np
import datetime
from pandas.tseries.offsets import *
from Funciones import YTD, var_anual, participacion_mercado, año_trailing

# TRAFICO DE Argentina
# https://datos.anac.gob.ar/estadisticas/

# TRAFICO DE BRASIL
# Extraigo los links de loz zip de la anac
# url_="http://www.anac.gov.br/assuntos/dados-e-estatisticas/demanda-e-oferta-do-transporte-aereo"
# response_ = requests.get(url_)
# content_=response_.content
# soup_ = BeautifulSoup(content_)
# links_=[]
# for i in soup.findAll('a', attrs={'href': re.compile("http://www.anac.gov.br/assuntos/dados-e-estatisticas/arquivos-demanda-e-oferta/")}):
#     links_.append(i.get("href"))
# # El primero de la lista debiera ser el último mes disponible, abro el zip
# links_[0]
# response = requests.get(links_[0])
# zip_file = ZipFile(BytesIO(response.content))
# files = zip_file.namelist()
# archivo=[str(i) for i in files if "xlsx" in str(i)]

def links_pagina(url, clave_busqueda):
    links=[]
    response = requests.get(url)
    content=response.content
    soup = BeautifulSoup(content)
    for i in soup.findAll('a', attrs={'href': re.compile(clave_busqueda)}):
        links.append(i.get("href"))
    return links

# TRAFICO DE CHILE
# En la página de la JAC me sirven dos archivos, de totales por línea aérea nacional e internacional
url_chile="http://www.jac.gob.cl/estadisticas/estadisticas-historicas/"
url_chile_nacional=links_pagina(url_chile, "Totales-por-operadores-Nacional")
url_chile_internacional=links_pagina(url_chile, "Totales-por-operadores-Internacional")
url_chile_nacional
# Transpongo mi base de datos para usar las columnas
xl_chl_nac=pd.read_excel("http://www.jac.gob.cl/"+url_chile_nacional[0], header=None, index=[0])
xl_chl_nac=xl_chl_nac.set_index([0])
aux_chile=xl_chl_nac.T.loc[1:12]
aux_chile
# Creo una función con los links que me sirven y los nombres a asignar, esta se mete a cada uno de las páginas y saca la tabla. Además de transponerla
# Las tablas se ubican entre operadores y total general, por lo que extraigo la posición de estas variables (inicio y final tabla) y creo nuevas tablas.
# Obtengo el año en cuestión usando el link de la página, este lo uso como índice para mis tablas buscando un n números que se encuentren entre / y -
# Trabajo con n tablas, para lo que creo una lista de listas de largo n y le anexo las n tablas paralelamente
def historico_chile(lista_links, num_tablas):
    lista_de_listas=[[] for l in range(num_tablas)]
    for i in lista_links:
        excel=pd.read_excel("http://www.jac.gob.cl/"+i, header=None, index=[0])
        excel=excel.set_index([0])
        excel=excel.T.loc[1:12]
        excel.columns=excel.columns.astype(str)
        excel.columns = [x.upper() for x in excel.columns]
        excel.rename(columns=lambda e: e.rstrip(), inplace=True)
        inicio_tabla = np.where(excel.columns == "OPERADORES")[0]
        final_tabla = np.where(excel.columns == "TOTAL GENERAL")[0]
        select=[]
        año=re.findall("(?<=/)[0-9]*(?=-)", i)[0]
        for t in range(len(inicio_tabla)):
            tabla=excel.iloc[:, inicio_tabla[t]+1:final_tabla[t]+1]
            tabla=tabla.set_index(pd.date_range(año+'-01-01', periods=12, freq='M'))
            select.append(tabla)
        for a,z in enumerate(lista_de_listas):
            z.append(select[a])
    return lista_de_listas
hist_chile_nac=historico_chile(url_chile_nacional[:6], 4)
hist_chile_int=historico_chile(url_chile_internacional[:6], 12)

def historico_anual_chile(link, num_tablas):
    excel=pd.read_excel("http://www.jac.gob.cl/"+link, header=None, index=[0])
    año_inicio=str(int(excel.iloc[5,1]))
    periodos=len(excel.iloc[5])-1
    excel=excel.set_index([0])
    excel=excel.T.loc[1:12]
    excel.columns=excel.columns.astype(str)
    excel.columns = [x.upper() for x in excel.columns]
    excel.rename(columns=lambda e: e.rstrip(), inplace=True)
    inicio_tabla = np.where(excel.columns == "OPERADORES")[0]
    final_tabla = np.where(excel.columns == "TOTAL GENERAL")[0]
    select=[]
    for t in range(len(inicio_tabla)):
        tabla=excel.iloc[:, inicio_tabla[t]+1:final_tabla[t]+1]
        dateRange = pd.date_range(año_inicio+'-01-01', periods=10, freq='Y')
        tabla=tabla.set_index(dateRange)
        select.append(tabla)
    return select

historico_anual_chile(url_chile_nacional[-1], 4)
historico_anual_chile(url_chile_internacional[-1], 12)

# Me interesan las tablas de pasajeros [posición nac.: 0, pos. int.:2], toneladas [1, 5], tráfico PAX [2, 8] y tráfico carga toneladas [3, 11]. Ojo que considero las llegadas+salidas para el internacional, pensar si es mejor usar otra tabla.
# Concateno las tablas de los diferentes años, dado que se encuentran en la misma posición relativa en cada uno de los excel.
# Latam Airlines se compone de varias compañías, por lo que las consolido. Saco todos los nombres con los que reporta, los sumo bajo un único nemo y borro las demás columnas.
# La función retorna un diccionario con lo un df con el nombre de lo que representa.
nombres_nac_cl=["pax_nac_cl", "ton_nac_cl", "rpk_nac_cl", "rtk_nac_cl"]
nombres_int_cl=["pax_int_cl", "ton_int_cl", "rpk_int_cl", "rtk_int_cl"]
def concatena_latam(base, nombres, n):
    lista=[]
    nuevo_dict={}
    for num in range(4):
        lista.append(pd.concat(base[num+n*(num*2+2)]).sort_index())
    for i,x in enumerate(lista):
        nombres_posibles=[n for n in x.columns if "LATAM" in n or "LAN" in n or "T.A.M" in n]
        x["LATAM AIRLINES"]=x.loc[:,nombres_posibles].sum(axis=1)
        x=x.loc[:,x.columns.difference(nombres_posibles)]
        nuevo_dict[nombres[i]]=x
    return nuevo_dict

dict_nac_cl=concatena_latam(hist_chile_nac, nombres_nac_cl, 0)
dict_int_cl=concatena_latam(hist_chile_int, nombres_int_cl, 1)

# Como son muchas aerolíneas y varían a lo largo de los años, para simplificar el análisis dejo sólo a latam, sky y jetsmart en nacional y sólo Latam en internacional.
# Jetsmart no existe para carga, por lo que hubo que borrarlo en esos df.
# Borro el resto de aerolíneas y las agrupo como otros.
comp_nac_cl=['JETSMART SPA', 'LATAM AIRLINES', 'SKY AIRLINE']
comp_int_cl=['LATAM AIRLINES']
def simplificando(diccionario, competidores):
        nuevo_dict={}
        lista_con_total=[]
        lista_con_total=competidores+["TOTAL GENERAL"]
        for k,v in diccionario.items():
            if "ton" in k or "rtk" in k:
                nuevo_dict[k]=diccionario[k]
                if 'JETSMART SPA' in competidores:
                    competidores_aux=competidores.copy()
                    competidores_aux.remove('JETSMART SPA')
                    nuevo_dict[k]=nuevo_dict[k].reindex(lista_con_total, axis=1).dropna(axis=1, how="all")
                    nuevo_dict[k]["OTROS"]=nuevo_dict[k]["TOTAL GENERAL"]-nuevo_dict[k][competidores_aux].sum(axis=1)
                    nuevo_dict[k].rename(columns=lambda x: x.split(" ")[0], inplace=True)
                    nuevo_dict[k]=nuevo_dict[k].replace({0:np.nan})
                    nuevo_dict[k]=nuevo_dict[k].dropna(axis=0, how="all")
            else:
                nuevo_dict[k]=diccionario[k]
                nuevo_dict[k]=nuevo_dict[k].reindex(lista_con_total, axis=1).dropna(axis=1, how="all")
                nuevo_dict[k]["OTROS"]=nuevo_dict[k]["TOTAL GENERAL"]-nuevo_dict[k][competidores].sum(axis=1)
                nuevo_dict[k].rename(columns=lambda x: x.split(" ")[0], inplace=True)
                nuevo_dict[k]=nuevo_dict[k].replace({0:np.nan})
                nuevo_dict[k]=nuevo_dict[k].dropna(axis=0, how="all")
        return nuevo_dict
trafico_chile_nac=simplificando(dict_nac_cl, comp_nac_cl)
trafico_chile_int=simplificando(dict_int_cl, comp_int_cl)
trafico_chile_int["rpk_int_cl"]
trafico_chile_nac["rpk_nac_cl"]

YTD(trafico_chile_int["rpk_int_cl"],datetime.date(2019, 1, 31),datetime.date(2019, 10, 30))
participacion_mercado(trafico_chile_int["rpk_int_cl"])
participacion_mercado(año_trailing(trafico_chile_int["rpk_int_cl"]))
participacion_mercado(año_trailing(trafico_chile_nac["rpk_nac_cl"]))
var_anual(trafico_chile_nac["rpk_nac_cl"])
var_anual(trafico_chile_int["rpk_int_cl"])
participacion_mercado(trafico_chile_nac["rpk_nac_cl"])
var_anual(trafico_chile_nac["rpk_nac_cl"]["LATAM"]).plot()
