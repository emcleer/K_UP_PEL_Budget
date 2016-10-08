# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 13:38:34 2016

@author: emcleer
"""

import pandas as pd
import sqlite3
import numpy as np

def read_test_mailing():
    ''' Just a test mailing saved out to .csv file -- will utilize this function with various mail dates to 
        test the functionality of potential model functions with ability to spot check along the way;
        Function returns a dataframe of mailings given the csv file provided '''
    
    df = pd.read_csv(r'I:\FINANCE\BUDGET\2017\PEL\20160420_mailing.csv', parse_dates=['mail_dte', 'initial_defect_dte', 'initial_tracked_dte'])
#    df2 = df[['lt', 'mob', 'liv_dec', 'mail_dte']].copy()
    df['mob'] = df['mob'].astype(int)
#    df2 = df2[df2['mob'] >= 0]
    return df
df = read_test_mailing()
df2 = df.groupby(['liv_dec'])['lt'].count()

def submittals():
    db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
    conn = sqlite3.connect(db)
    sql = ''' SELECT * FROM pel_submittals '''
    df = pd.read_sql(sql, conn, parse_dates=['submittal_dte'])
    df['lt'] = df['lt'].astype(int)
    return df

def nested_mail_doc(typ):
    df2 = read_test_mailing()
#    df2['resp'] = np.where(np.logical_and(df2['initial_tracked_dte'].notnull(), 
#                            np.logical_and(df2['mail_dte'] < df2['initial_tracked_dte'], df2['initial_tracked_dte'] <= df2['cutoff'])), 
#                            'yes', 'no')
#    df2['resp_days'] = np.where(df2['resp'] == 'yes', np.floor((df2['initial_tracked_dte'] - df2['mail_dte']) / np.timedelta64(1, 'D')), 0)
#    df2['resp_weeks'] = np.ceil(df2['resp_days'] / 7)
#    df2['defect'] = np.where(np.logical_and(df2['initial_defect_dte'].notnull(), 
#                            np.logical_and(df2['mail_dte'] < df2['initial_defect_dte'], df2['initial_defect_dte'] <= df2['cutoff'])), 
#                            'yes', 'no')    
    
    df_dict = {}
    
    for i in typ:
        df_dict[i] = df2[df2['liv_dec'] == i].copy()
    
    return df_dict

def resp_to_submittal_dicts(d, typ, subs):
    '''Takes mail and docfiniti merged df, in a nested dict from mail_with_liv_dec_mob, and will return
       a new nested dict after applying submittal information, from submittals (pulling from sqlite), to
       be used to build out response to submittal curves, by living/deceased and by defect yes/no'''
    
    defect_types = ('yes', 'no')
    defect_dict = {}
    
    for i in typ:
        df = d[i].copy()
        df2 = df[df['resp'] == 'yes'].copy()
        df3 = df2[['job','lt','ta_x','liv_dec','mob','elec_type','initial_defect_dte', 'initial_tracked_dte',
                      'estimated_fee_value', 'resp', 'resp_weeks', 'defect']].copy()
        df4 = df3.merge(subs, how='left', left_on='lt', right_on='lt').copy()
#        defect_dict[i] = df4
        df4['sub'] = np.where(df4['submittal_dte'].notnull(), 'yes', 'no')
        temp_dict = {}
        for j in defect_types:
            df5 = df4[df4['defect'] == j].copy()
            if j == 'yes':
                df5['sub_days'] = np.where(df5['sub'] == 'yes', np.floor((df5['submittal_dte'] - df5['initial_defect_dte']) / np.timedelta64(1, 'D')), 0)
                df5['sub_weeks'] = np.ceil(df5['sub_days'] / 7)
                df5['sub_yrmo'] = np.where(df5['sub'] == 'yes', df5['submittal_dte'].dt.strftime('%Y%m'), 'n/a')
            else:
                df5['sub_days'] = np.where(df5['sub'] == 'yes', np.floor((df5['submittal_dte'] - df5['initial_tracked_dte']) / np.timedelta64(1, 'D')), 0)
                df5['sub_weeks'] = np.ceil(df5['sub_days'] / 7)
                df5['sub_yrmo'] = np.where(df5['sub'] == 'yes', df5['submittal_dte'].dt.strftime('%Y%m'), 'n/a')
            
            temp_dict['defect_' + j] = df5
        
        defect_dict[i] = temp_dict
    
    return defect_dict

def test_rollup(d, typ, def_typ):
    ''' Rollup actuals by units and actual fees ''' 
    
    sub_units_dict = {}
    for i in typ:
        temp_dict = {}
        for j in def_typ:
            df = d[i][j]
            df2 = df.groupby(['sub_yrmo', 'sub']).agg({'lt':'count', 'rev_amt':'sum'}).copy()
            df3 = df2.reset_index()
            temp_dict[j] = df3
        sub_units_dict[i] = temp_dict
    return sub_units_dict

def test_output(d, typ, def_typ):
    ''' Takes the rollup dictionary from sub_units_rollup and writes each individual result to it's own tab in excel '''
    
    writer = pd.ExcelWriter(r'I:\FINANCE\BUDGET\2017\PEL\20160420_pelmodel_actuals.01.xlsx'#, datetime_format = 'YYYYMMDD'
                               )
    for i in typ:
        for j in def_typ:
            df = pd.DataFrame(d[i][j])
            df.to_excel(writer, '{}_{}'.format(i,j))
    writer.save()

typ = ('living', 'deceased')
def_typ = ('defect_yes', 'defect_no')
#test_mail = read_test_mailing()
subs = submittals()
resp_dict = nested_mail_doc(typ)
defect_dict = resp_to_submittal_dicts(resp_dict, typ, subs)
final = test_rollup(defect_dict, typ, def_typ)
test_output(final, typ, def_typ)

