# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 15:15:18 2016

@author: emcleer
"""

import pandas as pd
import sqlite3

db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
conn = sqlite3.connect(db)

pel_strip_bulk_sql = '''
                     select p.submittal_dte / 100 dte, p.lt, p.job, p.case_number, p.rev_amt
                     from pel_submittals p
                     where not exists (
                                      select l.case_number
                                      from lcs_submittals_all l
                                      where l.submittal_type = 'PEL Bulk Submittal' and l.service_provided in ('Deep Search', 'DRO')
                                      and l.case_number = p.case_number
                     )
                     and dte >= 201607
                     '''
def dte_lookup(s):
    dates = {date:pd.to_datetime(date, errors='coerce') for date in s.unique()}
    return s.map(dates)

def get_docfiniti_data():
    
    use_cols = ['approve_dte', 'curr_status', 'div_amt', 'elec_type', 'has_defect', 'is_approved', 'is_resolved_defect', 'job_number', 
                'last_defect_dte', 'lt_number', 'num_approved', 'num_defect', 'num_resolved_defect', 'ta', 'total_value', 'initial_defect_dte', 
                'initial_tracked_dte', 'last_tracked_dte', 'num_of_defects', 'times_defected', 'estimated_fee_value']
    
    df = pd.read_csv(r'I:\FINANCE\BUDGET\2017\PEL\Defect_Analysis_20160920.00.csv', encoding='latin-1', usecols=use_cols,
                      dtype={'approve_dte':str, 'last_defect_dte':str, 'initial_defect_dte':str, 'initial_tracked_dte':str,
                             'last_tracked_dte':str})
    
    dte_cols = ['approve_dte', 'last_defect_dte', 'initial_defect_dte', 'initial_tracked_dte', 'last_tracked_dte']
    
    for i in dte_cols:
        df[i] = dte_lookup(df[i])
    
    return df

def merge_data_sources(sql):
    doc = get_docfiniti_data()
    subs = pd.read_sql(sql, conn)
    subs['lt'] = subs['lt'].astype(int)
    
    df = subs.merge(doc, how='left', left_on='lt', right_on='lt_number')
    return df
    
df = merge_data_sources(pel_strip_bulk_sql)
df.to_clipboard()