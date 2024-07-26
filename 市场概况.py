import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, colors, Font, Border
from openpyxl.drawing.image import Image
from CommonFunctions import conn2db

wind_conn = conn2db('wind_conn')
factors_conn = conn2db('factors_conn')
market_conn = conn2db('market_conn')

# 左闭右闭，时间也自动化
begin_date = (pd.to_datetime(datetime.datetime.now())-pd.Timedelta('7 days')).strftime('%Y%m%d')
end_date = (pd.to_datetime(datetime.datetime.now())-pd.Timedelta('1 days')).strftime('%Y%m%d')

index = ['000832.CSI', '000300.SH', '399006.SZ', '000905.SH', '399303.SZ']
index_name = ['中证转债', '沪深300', '创业板指数', '中证500', '国证2000']
index_return = []

df_index1 = (pd.read_sql(
    "select TRADE_DT, S_DQ_CLOSE from WANDE.wande.dbo.CBINDEXEODPRICES where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, end_date, index[0]), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
index_return.append((df_index1.iloc[-1, 1] / df_index1.iloc[0, 1] - 1) * 100)
df_index2 = (pd.read_sql(
    "select TRADE_DT, S_DQ_CLOSE from WANDE.wande.dbo.AINDEXEODPRICES where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, end_date, index[1]), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
index_return.append((df_index2.iloc[-1, 1] / df_index2.iloc[0, 1] - 1) * 100)
df_index3 = (pd.read_sql(
    "select TRADE_DT, S_DQ_CLOSE from WANDE.wande.dbo.AINDEXEODPRICES where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, end_date, index[2]), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
index_return.append((df_index3.iloc[-1, 1] / df_index3.iloc[0, 1] - 1) * 100)
df_index4 = (pd.read_sql(
    "select TRADE_DT, S_DQ_CLOSE from WANDE.wande.dbo.AINDEXEODPRICES where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, end_date, index[3]), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
index_return.append((df_index4.iloc[-1, 1] / df_index4.iloc[0, 1] - 1) * 100)
df_index5 = (pd.read_sql(
    "select TRADE_DT, S_DQ_CLOSE from WANDE.wande.dbo.AINDEXEODPRICES where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, end_date, index[4]), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
index_return.append((df_index5.iloc[-1, 1] / df_index5.iloc[0, 1] - 1) * 100)

sector_index = []
sector_name = []
sector_return = []
for i in range(len(sector_index)):
    df_index = (pd.read_sql(
    "select TRADE_DT, S_DQ_CLOSE from WANDE.wande.dbo.CBINDEXEODPRICES where TRADE_DT>='%s' AND \
        TRADE_DT <= '%s' AND S_INFO_WINDCODE='%s'" %(
        begin_date, end_date, sector_index[i]), wind_conn)).sort_values(by=["TRADE_DT"], ignore_index=True)
    sector_return.append((df_index.iloc[-1, 1] / df_index.iloc[0, 1] - 1) * 100)
sector_rank = pd.DataFrame({"sector": sector_return,
                            "return": sector_return})
sector_rank = sector_rank.sort_values(by='return', ascending=False)
selected_sector = sector_rank.iloc[:3, :]

print()
'''
df_index = pd.merge(left = df_index1, right = df_index2, left_index = True, right_index = True)
df_index.index = index_name
df_index.columns = ['一周涨跌幅（%）', '年初至今涨跌幅（%）']
df_index = round(df_index, 2)
'''


