import pandas as pd
import random

#Custom libraries
from error_management import *
from redshift_functions import *


dfoutput=pd.DataFrame(data=None, index=None, columns=('table','column','validation','sqlstmt','result','comments','source_value','target_value'), dtype=None, copy=False)

#                    _
#                   | |
#   _____  _____ ___| |
#  / _ \ \/ / __/ _ \ |
# |  __/>  < (_|  __/ |
#  \___/_/\_\___\___|_|
#

def save_xls(df,p_full_path_xlsx,p_sheet_name='validation'):
	suffix = random.random().__str__()[2:6] #adding a random 4 digit number to the file to avoid error when writing if the file is open, and have a differnet version of the file
	#f=suffix.join([p_file_name,'.xlsx'])

	try:
		with pd.ExcelWriter(p_full_path_xlsx, engine='xlsxwriter') as writer:  # doctest: +SKIP
			df.to_excel(writer, sheet_name=p_sheet_name)
			writer.save()         # Close the Pandas Excel writer and output the Excel file.
			print('Saved excel file to disk:',p_full_path_xlsx)

	except:
		print('Problem saving xls file to disk, path and sheet name provided are:',p_full_path_xlsx,p_sheet_name)
		exception_handler()

def add_log_row(df,p_table_name,p_column_name,p_validation,p_result,p_comments,p_source_value,p_target_value,p_sqlstmt):
	try:
		dict1 = {'table': [p_table_name], 'column': [p_column_name], 'validation':[p_validation], 'sqlstmt':[p_sqlstmt],'result': [p_result], 'comments': [p_comments],'source_value':[p_source_value],'target_value':[p_target_value]}
		return(df.append(pd.DataFrame(dict1)))
	except:
		print('Problem adding log row to the data frame.',p_table_name,p_column_name)
		exception_handler()



