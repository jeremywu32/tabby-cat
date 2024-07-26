import numpy as np
import pandas as pd

from CommonFunctions import conn2db

wind_conn = conn2db('wind_conn')
factors_conn = conn2db('factors_conn')
market_conn = conn2db('market_conn')

# 万得可转债双低指数(889047.WI)持仓加权价格112（近两年历史最低点112），持仓债底103.83，债底溢价率7.88%。处于近一年0%分位。

