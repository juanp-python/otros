import pandas as pd
import numpy as np
pd.options.display.float_format = '{:,.0f}'.format

liquidez=pd.read_csv("otros/ffmm_8401_20200520_20200619.txt", sep=";")

money_market=pd.read_csv("otros/ffmm_8945_20200520_20200619.txt", sep=";")
money_market
money_market.PATRIMONIO_NETO.iloc[-1]/money_market.PATRIMONIO_NETO[0]
money_market.VALOR_CUOTA.iloc[-1]/money_market.VALOR_CUOTA[0]
money_market.ACTIVO_TOT.iloc[-1]

deuda_360=pd.read_csv("otros/ffmm_9056_20200520_20200619.txt", sep=";")
deuda_360.PATRIMONIO_NETO.iloc[-2]/deuda_360.PATRIMONIO_NETO[0]
deuda_360.VALOR_CUOTA.iloc[-1]/deuda_360.VALOR_CUOTA[0]
deuda_360.columns
deuda_360.ACTIVO_TOT.iloc[-1]

index_fund=pd.read_csv("otros/ffmm_8912_20200520_20200619.txt", sep=";")
index_fund.PATRIMONIO_NETO.iloc[-2]/index_fund.PATRIMONIO_NETO[0]
index_fund.PATRIMONIO_NETO.iloc[-1]
index_fund.ACTIVO_TOT.iloc[-1]

gold=pd.read_csv("otros/ffmm_8118_20200520_20200619.txt", sep=";")


selectivo=pd.read_csv("otros/ffmm_8490_20200520_20200619.txt", sep=";")
selectivo.ACTIVO_TOT.iloc[-1]

import dtale
dtale.show(money_market)
money_market.pivot_table(index="FECHA_INF", values=["ACTIVO_TOT", "PATRIMONIO_NETO"], aggfunc={"ACTIVO_TOT":np.mean, "PATRIMONIO_NETO":np.sum})
liquidez.pivot_table(index="FECHA_INF", values=["ACTIVO_TOT", "PATRIMONIO_NETO"], aggfunc={"ACTIVO_TOT":np.mean, "PATRIMONIO_NETO":np.sum})
deuda_360.pivot_table(index="FECHA_INF", values=["ACTIVO_TOT", "PATRIMONIO_NETO"], aggfunc={"ACTIVO_TOT":np.mean, "PATRIMONIO_NETO":np.sum})
gold.pivot_table(index="FECHA_INF", values=["ACTIVO_TOT", "PATRIMONIO_NETO"], aggfunc={"ACTIVO_TOT":np.mean, "PATRIMONIO_NETO":np.sum})
gold.MONEDA
money_market.MONEDA
