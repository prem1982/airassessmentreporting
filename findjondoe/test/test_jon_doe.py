######################################################################
# 
# (c) Copyright American Institutes for Research, unpublished work created 2013 
#  All use, disclosure, and/or reproduction of this material is 
#  prohibited unless authorized in writing. All rights reserved. 
# 
#  Rights in this program belong to: 
#   American Institutes for Research. 
# 
######################################################################
import pyodbc as p

from airassessmentreporting.findjondoe import find_jon_doe

# server = 'DC1PHILIPALT\SQLEXPRESS'
# database = 'Share'
# connStr = ( r'DRIVER={SQL Server};SERVER=' +
#             server + ';DATABASE=' + database + ';' +
#             'Trusted_Connection=yes'    )
# db_connection = p.connect(connStr)
# 
# """
# Parameters for find_jon_doe
# """
# needles_table = 'invals';  
# haystack_table = 'OGT1'
# 
# ## Dictionary of nominal and bignominal variables and their weights
# table_variables_dict = {} 
# table_variables_dict['nominal'] = \
#     {'dob_day' : 1, 
#      'dob_month' : 1, 
#      'dob_year' : 1}
#     
# table_variables_dict['bignominal'] = \
#     {'lithocode' : 1.5, 
#      'SSID' : 1.5, 
#      'Studentid' : 1.5, 
#      'dcrxid_attend' : 1.5, 
#      'bcrxid_attend' : 1.5 }
# name = ['ucrxfnm', 'ucrxlnm']
# name_weight = 1.5
# out_table = 'finds'
# out_variables = ['lithocode', 'SSID', 'ucrxfnm', 'ucrxlnm']
# top20_key_variable = 'SSID'
# cut = 10e-17
# count = 2 
# 
# find_jon_doe(db_connection, needles_table, haystack_table, \
#         table_variables_dict, \
#         name, name_weight,\
#         out_table, out_variables,\
#         top20_key_variable, \
#         cut,\
#         count)
#         
# db_connection.close()