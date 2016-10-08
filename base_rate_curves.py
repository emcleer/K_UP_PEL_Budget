# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 13:03:16 2016

@author: emcleer
"""

''' This version will have one curve for defect to submittals instead of breaking into an Age by Wks Out Matrix '''

import pandas as pd
import sqlite3
import numpy as np

def get_mailings(start, end):    
    db = r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\WIP\xlwings_test\mail.sqlite'
    conn = sqlite3.connect(db)
    sql = ''' SELECT * FROM mailed 
              WHERE mail_dte >= ?
              AND mail_dte <= ?
              AND line_of_bus = 'PEL'
          '''
    df = pd.read_sql(sql, conn, params=(start,end), parse_dates=['mail_dte'])
    df['lt'] = df['lt'].astype(int)
    df['cutoff'] = df['mail_dte'] + pd.Timedelta(weeks=9)
    return df

def dte_lookup(s):
    dates = {date:pd.to_datetime(date, errors='coerce') for date in s.unique()}
    return s.map(dates)
    
def get_docfiniti_data():
    
    use_cols = ['approve_dte', 'curr_status', 'div_amt', 'elec_type', 'has_defect', 'is_approved', 'is_resolved_defect', 'job_number', 
                'last_defect_dte', 'lt_number', 'num_approved', 'num_defect', 'num_resolved_defect', 'ta', 'total_value', 'initial_defect_dte', 
                'initial_tracked_dte', 'last_tracked_dte', 'num_of_defects', 'times_defected', 'estimated_fee_value']
    
    df = pd.read_csv(r'I:\FINANCE\BUDGET\2017\PEL\Defect_Analysis_20160822.01.csv', encoding='latin-1', usecols=use_cols,
                      dtype={'approve_dte':str, 'last_defect_dte':str, 'initial_defect_dte':str, 'initial_tracked_dte':str,
                             'last_tracked_dte':str})
    
    dte_cols = ['approve_dte', 'last_defect_dte', 'initial_defect_dte', 'initial_tracked_dte', 'last_tracked_dte']
    
    for i in dte_cols:
        df[i] = dte_lookup(df[i])
    
    return df

def get_wip():
    col_names = [
        'NMJOB#'
      , 'NMLT#'
      , 'NMACCT'
      , 'NMRPT#'
      , 'NMBLK#'
      , 'NMITM#'
      , 'NMTAX#'    # SSN 
      , 'NMRPO'
      , 'NMRPOD'
      , 'NMST'
      , 'NMZIP'
      , 'NMPHSH'
      , 'NMDHSH'
      , 'NMPFEE'
      , 'NMBFEE'
      , 'NMPRDT'     # Process date
      , 'NMP2DT'     # Process date2 (use reg process date field for submittal)
      , 'NMCKDT'     # Check date
      , 'NMPYDT'
      , 'NMADDT'     # Add Date
      , 'NMDOB'
      , 'NMPFP'
      , 'NMFMDT'
      , 'NMSMDT'
      , 'NMDOAI'     # Deceased indicator
      , 'NMRLCD'
      , 'NMPFEE1'    # Div Fee
      , 'NMDCKDT'    # Div Check Date
      , 'NMCCKDT'    # Cash Check Date
      , 'NMGEO'
      , 'NMDODT'     # Date of death
      , 'NMEADR'
      , 'NMDEDTE'    # Data Enter Date
      , 'NMCASE#'    # Keane case number
      , 'NMLSTACTDT' # Last activity date
      , 'NMLCSFLAG'
      , 'NMSUBMDTE'  # Submittal date (many blank - use Process Date)
      , 'NMELIG'     # Eligible? Y/N
      , 'NMLTRCD'    # Letter code
      , 'NMSTATUS'   # Status Code
      , 'NMTOTVAL'
      , 'DECVALFLAG' # Deceased Valid (need to confirm what this means)
      , 'DECVALDATE' # Deceseased Valid (need to confirm what this means)
      , 'CNFDECFLAG' # Confirmed Dcd (need to confirm what this means)
      , 'NMRPO#'
      , 'NMDAMT'
      , 'NMPHSH'
      , 'NMDHSH'
      , 'NMHSHR'
      , 'NMTSHR'
      , 'NMISHR'
      , 'NMTARPODATE']
    
    use_cols = ['NMJOB#', 'NMLT#', 'NMRPO', 'NMRPOD', 'NMPFEE', 'NMBFEE', 'NMPFEE1', 'NMPRDT', 'NMADDT', 'NMDOAI', 'NMRLCD', 'NMLSTACTDT',
               'NMLCSFLAG', 'NMLTRCD', 'NMTOTVAL', 'DECVALFLAG', 'DECVALDATE', 'CNFDECFLAG', 'NMRPO#', 'NMELIG', 'NMTARPODATE']
    
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\WIP\eric\WIP20160731.csv', header=None, names=col_names, usecols=use_cols, 
                     dtype={'NMRPOD':str, 'NMPRDT':str, 'NMADDT':str, 'NMLSTACTDT':str, 'DECVALFLAG':str, 
                            'DECVALDATE':str, 'CNFDECFLAG':str})
    
    df['NMADDT'] = dte_lookup(df['NMADDT'])
    return df

def dec_status(row):
    # Necessary to reallocate '0' Letter Codes to either Living/Deceased
    if row['NMLTRCD'] == 1:
        return 'living'
    elif row['NMLTRCD'] == 2:
        return 'deceased'
    elif row['NMLTRCD'] == 3:
        return 'living'
    elif row['NMLTRCD'] == 6:
        return 'deceased'
    elif row['NMLTRCD'] == 0:
        if row['DECVALFLAG'] == 'Y' and row['DECVALDATE'] != 0 and row['CNFDECFLAG'] == 'Y' and row['NMRLCD'] not in ['TR', 'CO']:
            return 'deceased'
        else:
            return 'living'
    else:
        return 'other'

def mail_with_liv_dec_mob(mail, wip, doc, typ):
    df = mail.merge(wip, how='left', left_on='lt', right_on='NMLT#')
    df['liv_dec'] = df.apply(dec_status, axis=1)
    df['mob'] = np.where(df['NMADDT'].isnull(), 'n/a', np.floor((df['mail_dte'] - df['NMADDT']) / np.timedelta64(1, 'M')))
    
    df2 = df.merge(doc, how='left', left_on='lt', right_on='lt_number')
    df2['resp'] = np.where(np.logical_and(df2['initial_tracked_dte'].notnull(), 
                            np.logical_and(df2['mail_dte'] < df2['initial_tracked_dte'], df2['initial_tracked_dte'] <= df2['cutoff'])), 
                            'yes', 'no')
    df2['resp_days'] = np.where(df2['resp'] == 'yes', np.floor((df2['initial_tracked_dte'] - df2['mail_dte']) / np.timedelta64(1, 'D')), 0)
    df2['resp_weeks'] = np.ceil(df2['resp_days'] / 7)
    df2['defect'] = np.where(np.logical_and(df2['initial_defect_dte'].notnull(), 
                            np.logical_and(df2['mail_dte'] < df2['initial_defect_dte'], df2['initial_defect_dte'] <= df2['cutoff'])), 
                            'yes', 'no')    
    
    df_dict = {}
    
    for i in typ:
        df_dict[i] = df2[df2['liv_dec'] == i].copy()
    
    return df_dict, df2


typ = ('living', 'deceased')
doc = get_docfiniti_data()
mail_two = get_mailings('20151231', '20160731')
wip = get_wip()
mail_d_two, z = mail_with_liv_dec_mob(mail_two, wip, doc, typ)

#z[z['mail_dte'] == '2016-04-20'].groupby(['ta_x', 'liv_dec'])['lt'].count()
#z[(z['mail_dte'] == '2016-04-20') & (z['liv_dec'] != 'other')].to_clipboard()

def defect_split_percent(d, typ):
    pct_split = {}
    
    for i in typ:
        df = d[i]
        df2 = df[df['resp'] == 'yes'].copy()
        tot = len(df2)
        df3 = df2.groupby('defect')['lt'].count()
        df4 = df3.div(tot, axis=0)
        df4 = df4.add_prefix('defect_')
        pct_split[i] = df4
    
    return pct_split

def submittals():
    db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
    conn = sqlite3.connect(db)
    sql = ''' SELECT * FROM pel_submittals '''
    df = pd.read_sql(sql, conn, parse_dates=['submittal_dte'])
    df['lt'] = df['lt'].astype(int)
    return df

#subs = submittals()

def cum_resp_curves_over_multiple_mailings(d, typ):
    ''' Will combine all mailings over specified period instead of breaking
        into individual mailings like resp_curves_by_wks_out_by_mail_dte and
        will return an Age x Wks Out matrix of response curves from mailings '''
        
    resp_pcts_dict = {}
    
    for i in typ:
        df = d[i]
        df['mob'] = df['mob'].astype(float)
        df['mob'] = df['mob'].astype(int)
        
        resp = df[df['resp'] == 'yes'].groupby(['resp_weeks','mob'])['lt'].count()
        resp = resp.unstack(level=0, fill_value=0)
        resp = resp.reset_index()
                
        tot = df.groupby(['mob'])['lt'].count()
        tot = tot.reset_index()
                
        pcts = tot[tot['mob'] >= 0].merge(resp, how='left', on=['mob'])
        pcts.fillna(0, inplace=True)
        pcts.sort_values(by=['mob'], inplace=True)
        pcts.rename(columns={'lt':'total_cts_lt'}, inplace=True)
        pcts.iloc[:,2:] = pcts.iloc[:,2:].div(pcts['total_cts_lt'], axis=0)
        resp_pcts_dict[i] = pcts
                   
    return resp_pcts_dict

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
            else:
                df5['sub_days'] = np.where(df5['sub'] == 'yes', np.floor((df5['submittal_dte'] - df5['initial_tracked_dte']) / np.timedelta64(1, 'D')), 0)
                df5['sub_weeks'] = np.ceil(df5['sub_days'] / 7)
            
            temp_dict['defect_' + j] = df5
        
        defect_dict[i] = temp_dict
    
    return defect_dict

#d = resp_to_submittal_dicts(mail_d_two, typ, subs)
#d['living']['defect_yes']['mob'].astype(float)

def resp_to_submittal_curves(d, typ):
    '''Takes a nested dict, from resp_to_submittal_dicts, and returns resp to submittal curves by
       living/deceased and by defect yes/no -- there is also an average fee populated by living/deceased
       and by defect yes/no'''
    
    defect_types = ('defect_yes', 'defect_no')
    defect_dict = {}
    
    for i in typ:
        pcts_dict = {}
        fees_dict = {}
        for j in defect_types:
            df = d[i][j].copy()
            df['mob'] = df['mob'].astype(float)
            df['mob'] = df['mob'].astype(int)
            subs = df[df['sub'] == 'yes'].groupby('sub_weeks')['lt'].count()
            
            tot = df['lt'].count()
            
            pcts = subs.divide(tot)
            pcts_dict[j] = pcts
                       
            avg_fees = df[df['sub'] == 'yes']['rev_amt'].mean()
            fees_dict[j] = avg_fees
        
        defect_dict[i] = pcts_dict, fees_dict
    
    return defect_dict

#p = resp_to_submittal_curves(d, typ)
#p['deceased'][0]['defect_yes'].to_clipboard()

#p = cum_resp_curves_over_multiple_mailings(mail_d_two, typ)
#p['deceased'].to_clipboard()