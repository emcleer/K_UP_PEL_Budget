# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 09:45:48 2016

@author: emcleer
"""

import pandas as pd
import sqlite3
import numpy as np
from collections import defaultdict

def main():
    
    typ = ('living','deceased')
    def_typ = ('defect_yes', 'defect_no')
    curves, mail_cycles, new_accts, buckets = read_base_rate_curves()
    dnm = do_not_mail_jobs()
    mail = format_mail(typ)
    tmp_list_sub_cts = []
    tmp_list_mail_cts = []
    for i in typ:
        for j in mail[i].keys():
            for k in mail[i][j].columns:
                ser = mail[i][j][k].copy()
                resp, tot_mail = mail_schedule_roll(ser, curves, i, j, mail_cycles)
                tmp_list_mail_cts.append(tot_mail)
                resp_d = resp_unit_defect_split(resp, i, def_typ)
                subs_d = sub_units_by_weeks(resp_d, i, def_typ, curves)
                subs_f = sub_units_rollup(subs_d, i, def_typ)
                
                for m in def_typ:
                    df = subs_f[m].copy()
                    df2 = df.reset_index()
                    df2['type'] = i
                    df2['job'] = j
                    df2['defect_type'] = m
                    tmp_list_sub_cts.append(df2)
                
                    
    final_subs = pd.concat(tmp_list_sub_cts)
    final_subs['job_buckets'] = final_subs['job'].map(buckets)
    final_subs['job_status'] = np.where(final_subs['job'].isin(dnm), 'terminated', 'active')
    final_subs['strip_ast_assigned'] = np.where(np.logical_and(final_subs['job_buckets'] == 'AST_Assigned', final_subs['type'] == 'living'), 'living_cant_work', 'deceased_still_eligible')
    
    final_mail = pd.concat(tmp_list_mail_cts)
    final_mail['job_buckets'] = final_mail['job'].map(buckets)
    final_mail['job_status'] = np.where(final_mail['job'].isin(dnm), 'terminated', 'active')
    final_mail['strip_ast_assigned'] = np.where(np.logical_and(final_mail['job_buckets'] == 'AST_Assigned', final_mail['liv_dec'] == 'living'), 'living_cant_work', 'deceased_still_eligible')
    
    return final_subs, final_mail
                    
def read_base_rate_curves():
    ''' Reads Base_Rate_Curve file -- ideally this will work same way for full model
        except that these base rates will be read into a dictionary, and then input values will either 
        increase/decrease proporationately (or not, if so decided) across the matrix curves; 
        Function returns of dictionary of curve matrices with Age ('MOB') as the index '''
    
    fp = r'I:\FINANCE\BUDGET\2017\PEL\Base_Rate_Curves.01.xlsx'
    matrix_tabs = ['Liv_Resp', 'Dec_Resp']
    single_curve_tabs = ['Liv_Defect_Yes', 'Liv_Defect_No', 'Dec_Defect_Yes', 'Dec_Defect_No']
    curves_dict = {}
    
    for i in matrix_tabs:
        df = pd.read_excel(fp, sheetname=i)
        df2 = df.iloc[:-3, :-3].copy()
        df2['mob'] = df2['mob'].astype(int)
        df2.set_index('mob', inplace=True)
        df3 = df2.iloc[:,1:].copy()
        curves_dict[i] = df3
    
    for j in single_curve_tabs:
        df = pd.read_excel(fp, sheetname=j)
        df2 = df.iloc[:-3, :-3].copy()
        df3 = df2.iloc[:,1:].copy()
        curves_dict[j] = df3
    
    mail_cycles = pd.read_excel(fp, sheetname='Mail_Cycles')
    mail_cycles['job_number'] = mail_cycles['job_number'].astype(str)
    mail_cycles = mail_cycles.set_index('job_number')['days_between_mailings'].to_dict()
    
    new_accts = pd.read_excel(fp, sheetname='New_Accts')
    new_accts['job_number'] = new_accts['job_number'].astype(str)
    new_accts = new_accts.set_index('job_number')['new_accts_mailed_per_cycle'].to_dict()
    
    buckets = pd.read_excel(fp, sheetname='Job_Mapping')
    buckets['job_number'] = buckets['job_number'].astype(str)
    buckets = buckets.set_index('job_number')['job_buckets'].to_dict()
    
    return curves_dict, mail_cycles, new_accts, buckets
    
def read_mail_db():
    legacy_lkup = pd.read_csv(r'I:\FINANCE\BUDGET\2017\PEL\job_distinction_lookup.csv', dtype={'job_num':str})
    legacy_lkup_dict = legacy_lkup.set_index('job_num').to_dict()
    
    db = r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\WIP\xlwings_test\mail.sqlite'
    conn = sqlite3.connect(db)
    sql_ones = '''
              SELECT m.job, m.lt, m.mail_dte, (m.mail_dte / 100) mail_mo, m.ta, m.add_date, m.letter_code
              FROM mailed as m
              INNER JOIN (
                         SELECT job, max(mail_dte / 100) mail_dte, letter_code
                         FROM mailed
                         WHERE letter_code = 1
                         AND line_of_bus = 'PEL'
                         GROUP BY job
              ) x on (x.job = m.job AND x.mail_dte = mail_mo AND x.letter_code = m.letter_code)
              '''
    sql_twos = '''
              SELECT m.job, m.lt, m.mail_dte, (m.mail_dte / 100) mail_mo, m.ta, m.add_date, m.letter_code
              FROM mailed as m
              INNER JOIN (
                         SELECT job, max(mail_dte / 100) mail_dte, letter_code
                         FROM mailed
                         WHERE letter_code = 2
                         AND line_of_bus = 'PEL'
                         GROUP BY job
              ) x on (x.job = m.job AND x.mail_dte = mail_mo AND x.letter_code = m.letter_code)
              '''
    sql_threes = '''
              SELECT m.job, m.lt, m.mail_dte, (m.mail_dte / 100) mail_mo, m.ta, m.add_date, m.letter_code
              FROM mailed as m
              INNER JOIN (
                         SELECT job, max(mail_dte / 100) mail_dte, letter_code
                         FROM mailed
                         WHERE letter_code = 3
                         AND line_of_bus = 'PEL'
                         GROUP BY job
              ) x on (x.job = m.job AND x.mail_dte = mail_mo AND x.letter_code = m.letter_code)
              '''
    ones = pd.read_sql(sql_ones, conn, parse_dates=['mail_dte'])
    threes = pd.read_sql(sql_threes, conn, parse_dates=['mail_dte'])
    liv = pd.concat([ones, threes])
    dcd = pd.read_sql(sql_twos, conn, parse_dates=['mail_dte'])
    mail = pd.concat([liv, dcd])
    mail = mail[mail['add_date'].notnull()]
    mail['add_date'] = mail['add_date'].astype(str)
    mail['add_date'] = mail['add_date'].str.split('.').str[0]
    mail['add_date'] = pd.to_datetime(mail['add_date'])
    mail['mob'] = np.floor((mail['mail_dte'] - mail['add_date']) / np.timedelta64(1, 'M'))
    mail['liv_dcd'] = np.where(mail['letter_code'] == 2, 'deceased', 'living')
#==============================================================================
#     mail['tmp_job_class'] = mail['job'].map(legacy_lkup_dict['class'])
#        
#     mail['job_class'] = mail.apply(job_class, axis=1)
#==============================================================================
#==============================================================================
#     mail = mail.groupby(['job', 'letter_code'])['lt'].count().copy()
#     mail = mail.reset_index().copy()
#==============================================================================
    return mail

def job_class(row):
    if row['ta'] == 'Computershare Inc.':
        if row['job'] == '5001':
            return 'att'
        elif row['job'] > '5100' and row['job'] < '5200':
            return 'keane_direct'
        elif row['tmp_job_class'] == 'cs_legacy_direct':
            return row['tmp_job_class']
        else:
            return 'cs_legacy_assigned'
    
    elif row['ta'] == 'American Stock Transfer & Trust Company, LLC':
        if row['tmp_job_class'] == 'ast_direct':
            return row['tmp_job_class']
        else:
            return 'ast_legacy_assigned'
    
    elif row['ta'] == 'Wells Fargo Shareowner Services':
        if row['job'] == '300000':
            return 'comcast'
        else:
            return 'wf_others'
    
    elif row['ta'] == 'Broadridge Shareholder Services':
        return 'broadridge'
    
    elif row['ta'] == 'CST Trust Company':
        return 'cst'
    
    else:
        return 'misclassified'

def format_mail(typ):
    mail = read_mail_db()
    df_dict = {}
    for i in typ:
        df = mail[mail['liv_dcd'] == i].copy()
        tmp_dict = {}
        for j in df['job'].unique():
            df2 = df[df['job'] == j].copy()
            df3 = df2.groupby(['mob', 'mail_dte'])['lt'].count()
            df4 = df3.unstack(level='mail_dte', fill_value=0)
            if (df4.index >= 53).any() == True:             
                temp_sum = df4[df4.index >= 53].sum()
                df4 = df4[df4.index < 53].copy()
                df4.set_value(53, df4.columns[0], temp_sum[0])
            else:
                pass

            tmp_dict[j] = df4
        df_dict[i] = tmp_dict
    
    return df_dict

def mail_schedule_roll(s, curves, typ, job, mail_cycles):
    ''' Will need to cycle through output of format_mail() and pass individual Series into this, the typ and job class will come from 'i' and 'j' via the loop.
        Curves dict is from read_base_rate_curves -- index 0 will return a list of response unit dataframes while index 1 will return a total datafarme capturing
        the mail counts for the entire cycle (returning liv/dec, job_class, mail date, and pc count) '''
    
    mailed_list = []
    resp_list = []
    
    for j in range(8):                                 # need to link to xlwings for mail cnter input
    
#        new_accts = [2000, 2000, 2000]
        if j == 0:
            dte_cnter = s.name
        else:
            dte_cnter = dte_cnter
            
        if j == 0:
            mail = s.copy()
        else:
            mail = mail
        
        mailed_list.append([typ, job, dte_cnter, mail.sum()])
        
        if typ == 'living':
            resp = curves['Liv_Resp'].multiply(mail, axis='index')
        else:
            resp = curves['Dec_Resp'].multiply(mail, axis='index')
        
        resp.fillna(0, inplace=True)
        resp.rename(columns={k: dte_cnter + pd.Timedelta(weeks=int(k)) for k in resp.columns}, inplace=True)
        resp_list.append(resp.copy())

        dte_cnter += pd.Timedelta(days = mail_cycles[job])                      ## link this to xlwings input template to read different mailing cycle terms
        resp_tot = resp.sum(axis=1)
        mail -= resp_tot
        
        mail.index = mail.index + int(np.floor(mail_cycles[job] / 30))                             ## This shifts the Age of MOB 2 months later, which correlates with the next mailing from dte_cnter
        mail.set_value(0, 0)                                    ## Setting "MOB: 0" to 0 for test purposes -- this will need to be passed in via RB's new accts added per mailing
        mail.sort_index(inplace=True)
        
        if (mail.index >= 53).any() == True:                    ## This section will condense any ages >= 53 MOB to all be Age 53 as that is what our curve matrices index is
            temp_sum = mail[mail.index >= 53].sum()
            mail = mail[mail.index < 53].copy()
            mail.set_value(53, temp_sum)
        else:                                                   ## Should do something here if/when we get a an avearge ct of dropped accts per mailing -- should subtract from value of last index val
            pass

    mailed_df = pd.DataFrame(mailed_list, columns=['liv_dec', 'job', 'mail_dte', 'mail_cts'])
    mailed_df['mail_dte'] = pd.to_datetime(mailed_df['mail_dte'])
    
    return resp_list, mailed_df

def resp_unit_defect_split(d, typ, def_typ):
    ''' Takes a list of dataframe response units to split into defect yes/no populations;
        Function returns a list of pd.Series of response units with a datetime index, in a nested dictionary by defect type '''
    
    liv_def_yes = .229918                                            #needs to be updated so that this reads in from a file
    dec_def_yes = .751765                                            #needs to be updated so that this reads in from a file
          
    resp_units_dict = defaultdict(list)   
    
    for j in range(len(d)):
        df = d[j]       
        for k in def_typ:
            if typ == 'living':
                if k == 'defect_yes':
                    df2 = df.multiply(liv_def_yes)
                else:
                    df2 = df.multiply((1 - liv_def_yes))
            else:
                if k == 'defect_yes':
                    df2 = df.multiply(dec_def_yes)
                else:
                    df2 = df.multiply((1 - dec_def_yes))
            df3 = df2.sum(axis=0).copy()
            resp_units_dict[k].append(df3)
    return resp_units_dict

def sub_units_by_weeks(d, typ, def_typ, curves):
    ''' Takes the nested dictionary form resp_unit_defect_split and takes the response units, by defect type, by response week, 
        and multiplies those unit counts against the response to submittal single_curves -- this results in a new dataframe,
        created from the response week date, because a new curve timeline starts from response to submittal;
        Function returns a list of submittal unit dataframes in a nested dictionary, by living/deceased, then by defect type '''
           
    sub_units_dict = defaultdict(list)
    
    for j in def_typ:
        for k in range(len(d[j])):
            ser = d[j][k].copy()
            temp_list = []
            for m in range(len(ser)):
                if typ == 'living':
                    if j == 'defect_yes':
                        df2 = curves['Liv_Defect_Yes'].multiply(ser[m])
                    else:
                        df2 = curves['Liv_Defect_No'].multiply(ser[m])
                else:
                    if j == 'defect_yes':
                        df2 = curves['Dec_Defect_Yes'].multiply(ser[m])
                    else:
                        df2 = curves['Dec_Defect_No'].multiply(ser[m])
                df2.rename(columns={n: ser.index[m] + pd.Timedelta(weeks=int(n)) for n in df2.columns}, inplace=True)
                df2.fillna(0, inplace=True)
                temp_list.append(df2)
            sub_units_dict[j].append(temp_list)                
    return sub_units_dict
    
def sub_units_rollup(d, typ, def_typ):
    ''' Takes the nested dictionary from sub_units_by_weeks, and concats all dataframe lists into one df, it
        then transposes the datetimes in the columns to the index, creates a column to extract year-mo, and then
        groups by the total count of units by year-mo;
        Function returns a Series of total submittal counts, by year-mo in a nested dict by living/deceased, then by defect type '''
       
    sub_units_temp_dict = defaultdict(list)
    
    for j in def_typ:
        for k in range(len(d[j])):
            df_list = d[j][k].copy()
            df = pd.concat(df_list, axis=1).copy()
            sub_units_temp_dict[j].append(df)
    
    sub_units_dict = {}
    
    for j in def_typ:
        df_list_two = sub_units_temp_dict[j].copy()
        df2 = pd.concat(df_list_two, axis=1).copy()
        df3 = df2.T.copy()
        df3['total_ct'] = df3.sum(axis=1)
        df3['yearmo'] = df3.index.strftime('%Y%m')
        df4 = df3.groupby('yearmo')['total_ct'].sum().copy()            
        sub_units_dict[j] = df4
    
    return sub_units_dict

def sub_units_output(d, typ, def_typ):
    ''' Takes the rollup dictionary from sub_units_rollup and writes each individual result to it's own tab in excel '''
    
    writer = pd.ExcelWriter(r'I:\FINANCE\BUDGET\2017\PEL\test_3_mos.xlsx'#, datetime_format = 'YYYYMMDD'
                               )
    for i in typ:
        for j in def_typ:
            df = pd.DataFrame(d[i][j]).T
            df.to_excel(writer, '{}_{}'.format(i,j))
    writer.save()

def do_not_mail_jobs():
    fp = r'I:\OPERATIONS\PEL\PEL Data Analysis KR RB EM\Misc Files\job_master_all.csv'
    jobs = pd.read_csv(fp, encoding='latin1', usecols=['JOMLCDE', 'JOMLCDE', 'JOJOB#'])
    dnm = jobs[(jobs['JOMLCDE'] == 'N') | (jobs['JOMLCDE'].isnull())]['JOJOB#']
    r = set(str(_) for _ in dnm)
    return r

subs, mail = main()
subs.to_clipboard()