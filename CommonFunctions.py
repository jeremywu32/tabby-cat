# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 09:54:21 2023

@author: aichong
"""
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
# from sqlalchemy import DateTime, Integer, BigInteger, create_engine, Column, TIMESTAMP, \
#     String, func, TEXT, VARCHAR, \
#     DECIMAL, BIGINT, DATE, DATETIME, PrimaryKeyConstraint, Index, and_
from urllib.parse import quote_plus as urlquote

# from sqlalchemy.dialects.mysql import DOUBLE
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker
# factor_engine = create_engine(r'mysql+pymysql://zxjt:hjoj4POrbkcV2ZO7@192.168.108.191:3306/factors?charset=utf8')
# session_maker = sessionmaker(bind=factor_engine)
# session = session_maker()
# Base = declarative_base()

# import pdb

def conn2db(dbname):
    # connet to databases
    pwd = urlquote('789@zxjt')
    if 'wind_conn'==dbname:
        # master database to transfer to remote server
        dbe = create_engine(f"mssql+pyodbc://panrl:{urlquote('panrl@zxjt')}@192.168.108.191:1433/master?driver=SQL+Server").connect()
    elif 'info_conn'==dbname:
        # information_schema database in local server
        dbe = create_engine(r'mysql+pymysql://zxjt_read:%s@192.168.108.191:3306/information_schema?charset=utf8'%pwd).connect()
    elif 'zyyxbk_conn'==dbname:
        # zyyx_backup database in local server
        dbe = create_engine(r'mysql+pymysql://zxjt_read:%s@192.168.108.191:3306/zyyx_backup?charset=utf8'%pwd).connect()
    elif 'market_conn'==dbname:
        # market database in local server
        dbe = create_engine(r'mysql+pymysql://zxjt_read:%s@192.168.108.191:3306/market?charset=utf8'%pwd).connect()
    elif 'factors_conn'==dbname:
        # factors database in local server
        dbe = create_engine(r'mysql+pymysql://zxjt_read:%s@192.168.108.191:3306/factors?charset=utf8'%pwd).connect()
    elif 'factor_testing_conn'==dbname:
        # factor_testing database in local server
        dbe = create_engine(r'mysql+pymysql://zxjt_read:%s@192.168.108.191:3306/factor_testing?charset=utf8'%pwd).connect()
    elif 'factors_2024_conn'==dbname:
        # factor_testing database in local server
        dbe = create_engine(r'mysql+pymysql://zxjt_read:%s@192.168.108.191:3306/factors_2024?charset=utf8'%pwd).connect()
    
    return dbe

def conn2DataSource():
    # connet to DataSource
    from bqdatasdk import DataSource
    base_url = 'https://202.0.1.142:8888'
    token = "tk_a6f3fb5a2d7527bdefc59f4310ccce45"  # 取saas平台-模拟交易API-API Token https://aiquant.csc108.com/account/api
    DataSource.init(base_url, token)
    
    return DataSource

def update_tradying_days():
    # update trading_days from wind database to data/common dictionary
    import os, datetime
    datadir = os.environ.get('USER_DATA_DIR')
    # fetch from wind
    wind_conn = conn2db('wind_conn')
    sqlstr = "select TRADE_DAYS as date, S_INFO_EXCHMARKET from WANDE.wande.dbo.ASHARECALENDAR where S_INFO_EXCHMARKET='SSE';"
    tradying_days = pd.read_sql(sqlstr ,wind_conn).sort_values('date')
    tradying_days['date'] = pd.to_datetime(tradying_days['date'])
    current_year = datetime.datetime.now().year
    tradying_days = tradying_days[tradying_days['date'].dt.year<=(current_year+1)]
    tradying_days = tradying_days[['date']]
    
    # save
    tradying_days.to_parquet(datadir+'/common/tradying_days.pq')
    
    return tradying_days


def statistics_core_1(special_date, navs, index_prices, benchmark_code):
    '''This function makes statistics from navs_comb.'''
    # config
    # logger = self.logger
    # logger.info("----------------Start %s.%s().----------------" %(self.__class__.__name__,inspect.stack()[0][3]))
    # statistics
    factor_statistic = pd.DataFrame()
    factor_statistic['portfolio_id'] = navs.columns
    factor_statistic['trading_days'] = navs.shape[0]
    factor_statistic['ret_in_period'] = (navs.iloc[-1,:]-1).values*100
    idx_spc = navs.index.tolist().index(special_date)
    factor_statistic['ret_special_date'] = (navs.iloc[-1,:]/navs.iloc[idx_spc,:]-1).values*100
    if navs.shape[0]>242:
        factor_statistic['ret_recent1year'] = (navs.iloc[-1,:]/navs.iloc[-242,:]-1).values*100
    else:
        factor_statistic['ret_recent1year'] = np.nan
    if navs.shape[0]>121:
        factor_statistic['ret_recent6month'] = (navs.iloc[-1,:]/navs.iloc[-121,:]-1).values*100
    else:
        factor_statistic['ret_recent6month'] = np.nan
    if navs.shape[0]>64:
        factor_statistic['ret_recent3month'] = (navs.iloc[-1,:]/navs.iloc[-64,:]-1).values*100
    else:
        factor_statistic['ret_recent3month'] = np.nan
    factor_statistic['ret_recent1month'] = (navs.iloc[-1,:]/navs.iloc[-22,:]-1).values*100
    factor_statistic['ret_recent1week'] = (navs.iloc[-1,:]/navs.iloc[-6,:]-1).values*100
    factor_statistic['ret_recent3day'] = (navs.iloc[-1,:]/navs.iloc[-4,:]-1).values*100
    factor_statistic['ret_recent2day'] = (navs.iloc[-1,:]/navs.iloc[-3,:]-1).values*100
    factor_statistic['ret_recent1day'] = (navs.iloc[-1,:]/navs.iloc[-2,:]-1).values*100
    factor_statistic['annualized_ret'] = (navs.iloc[-1,:]**(242/navs.shape[0])-1).values*100
    factor_statistic['volatility'] = (np.log(navs).diff().std()*np.sqrt(242)).values*100
    factor_statistic['max_draw_down'] = (1-(navs/navs.cummax()).min()).values*100
    
    if len(benchmark_code)>0:
        # fetch index prices
        index_prices = index_prices[benchmark_code]
        index_rets = index_prices.pct_change()
        # adv_navs
        portfolio_rets = navs.pct_change()
        adv_navs = portfolio_rets.sub(index_rets.loc[portfolio_rets.index].values,axis=0)
        adv_navs.iloc[0,:] = 0
        adv_navs = (adv_navs+1).cumprod()
        # adv_navs.loc[adv_navs.index, icol] = adv_navs.loc[adv_navs.index,icol]
        
        # advanced return
        adv_statistic = pd.DataFrame()
        adv_statistic['portfolio_id'] = adv_navs.columns
        if navs.shape[0]>242:
            adv_statistic['adv_ret_recent1year'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-242,:]-1).values*100
        else:
            adv_statistic['adv_ret_recent1year'] = np.nan
        if navs.shape[0]>121:
            adv_statistic['adv_ret_recent6month'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-121,:]-1).values*100
        else:
            adv_statistic['adv_ret_recent6month'] = np.nan
        if navs.shape[0]>64:
            adv_statistic['adv_ret_recent3month'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-64,:]-1).values*100
        else:
            adv_statistic['adv_ret_recent3month'] = np.nan
        adv_statistic['adv_ret_recent1month'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-22,:]-1).values*100
        adv_statistic['adv_ret_recent1week'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-6,:]-1).values*100
        adv_statistic['adv_ret_recent3day'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-4,:]-1).values*100
        adv_statistic['adv_ret_recent2day'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-3,:]-1).values*100
        adv_statistic['adv_ret_recent1day'] = (adv_navs.iloc[-1,:]/adv_navs.iloc[-2,:]-1).values*100
        adv_statistic['annualized_adv_ret'] = (adv_navs.iloc[-1,:]**(242/adv_navs.shape[0])-1).values*100
        adv_statistic['adv_volatility'] = (np.log(adv_navs).diff().std()*np.sqrt(242)).values*100
        adv_statistic['adv_max_draw_down'] = (1-(adv_navs/adv_navs.cummax()).min()).values*100
        adv_statistic['adv_volatility_recent3month'] = (np.log(adv_navs.iloc[-64:,:]).diff().std()*np.sqrt(242)).values*100
        adv_statistic['adv_max_draw_down_recent3month'] = (1-(adv_navs.iloc[-64:,:]/adv_navs.iloc[-64:,:].cummax()).min()).values*100
        adv_statistic['adv_volatility_recent1month'] = (np.log(adv_navs.iloc[-21:,:]).diff().std()*np.sqrt(242)).values*100
        adv_statistic['adv_max_draw_down_recent1month'] = (1-(adv_navs.iloc[-21:,:]/adv_navs.iloc[-21:,:].cummax()).min()).values*100
        adv_statistic['adv_volatility_recent1week'] = (np.log(adv_navs.iloc[-5:,:]).diff().std()*np.sqrt(242)).values*100
        adv_statistic['adv_max_draw_down_recent1week'] = (1-(adv_navs.iloc[-5:,:]/adv_navs.iloc[-5:,:].cummax()).min()).values*100
        factor_statistic = factor_statistic.merge(adv_statistic, how='left', on=['portfolio_id'])
        
        # information ratio
        factor_statistic['information_ratio'] = factor_statistic['annualized_adv_ret']/factor_statistic['adv_volatility']
        factor_statistic['return_mdd_ratio'] = factor_statistic['annualized_adv_ret']/factor_statistic['adv_max_draw_down']
        factor_statistic['information_ratio_r3month'] = factor_statistic['adv_ret_recent3month']/factor_statistic['adv_volatility']*242/63
        factor_statistic['return_mdd_ratio_r3month'] = factor_statistic['adv_ret_recent3month']/factor_statistic['adv_max_draw_down']*242/63
        factor_statistic['information_ratio_recent3month'] = factor_statistic['adv_ret_recent3month']/factor_statistic['adv_volatility_recent3month']*242/63
        factor_statistic['return_mdd_ratio_recent3month'] = factor_statistic['adv_ret_recent3month']/factor_statistic['adv_max_draw_down_recent3month']*242/63
        factor_statistic['information_ratio_recent1month'] = factor_statistic['adv_ret_recent1month']/factor_statistic['adv_volatility_recent1month']*242/21
        factor_statistic['return_mdd_ratio_recent1month'] = factor_statistic['adv_ret_recent1month']/factor_statistic['adv_max_draw_down_recent1month']*242/21
        factor_statistic['information_ratio_recent1week'] = factor_statistic['adv_ret_recent1week']/factor_statistic['adv_volatility_recent1week']*242/5
        factor_statistic['return_mdd_ratio_recent1week'] = factor_statistic['adv_ret_recent1week']/factor_statistic['adv_max_draw_down_recent1week']*242/5
    
        # reformate
        factor_statistic = factor_statistic.sort_values(by=['information_ratio_r3month'],ascending=False).reset_index(drop=True)
    
    return factor_statistic