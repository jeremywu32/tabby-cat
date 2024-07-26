import numpy as np
import pandas as pd
import datetime

from CommonFunctions import conn2db

wind_conn = conn2db('wind_conn')
factors_conn = conn2db('factors_conn')
market_conn = conn2db('market_conn')

# 万得可转债双低指数(889047.WI)持仓加权价格112（近两年历史最低点112），持仓债底103.83，债底溢价率7.88%。处于近一年0%分位。

begin_date = (pd.to_datetime(datetime.datetime.now())-pd.Timedelta('484 days')).strftime('%Y%m%d')
intermediate_date = (pd.to_datetime(datetime.datetime.now())-pd.Timedelta('242 days')).strftime('%Y%m%d')
end_date = (pd.to_datetime(datetime.datetime.now())-pd.Timedelta('1 days')).strftime('%Y%m%d')

df1 = (pd.read_sql(
    "select TRADE_DT, S_CON_WINDCODE from WANDE.wande.dbo.CBINDEXWEIGHT where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, intermediate_date, '889047.WI'), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)

df2 = (pd.read_sql(
    "select TRADE_DT, S_CON_WINDCODE from WANDE.wande.dbo.CBINDEXWEIGHT where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        intermediate_date, end_date, '889047.WI'), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
