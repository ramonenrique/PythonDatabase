import pandas as pd
import random

dfoutput=pd.DataFrame(data=None, index=None, columns=('table','column','validation','sqlstmt','result','comments','source_value','target_value'), dtype=None, copy=False)

#                    _
#                   | |
#   _____  _____ ___| |
#  / _ \ \/ / __/ _ \ |
# |  __/>  < (_|  __/ |
#  \___/_/\_\___\___|_|
#

def save_xls(df,p_file_name):
	suffix = random.random().__str__()[2:6] #adding a random 4 digit number to the file to avoid error when writing if the file is open, and have a differnet version of the file
	f=suffix.join([p_file_name,'.xlsx'])
	with pd.ExcelWriter(f, engine='xlsxwriter') as writer:  # doctest: +SKIP
		df.to_excel(writer, sheet_name='validation')
		writer.save()         # Close the Pandas Excel writer and output the Excel file.
	print('Saved excel file to disk:',p_file_name)

def add_log_row(df,p_table_name,p_column_name,p_validation,p_result,p_comments,p_source_value,p_target_value,p_sqlstmt):
	#dict1 = {'table': p_table_name, 'column': p_column_name, 'validation':p_validation, 'sqlstmt':p_sqlstmt,'result': p_result, 'comments':p_comments,'source_value':p_source_value,'target_value':p_target_value}
	dict1 = {'table': [p_table_name], 'column': [p_column_name], 'validation':[p_validation], 'sqlstmt':[p_sqlstmt],'result': [p_result], 'comments': [p_comments],'source_value':[p_source_value],'target_value':[p_target_value]}
	return(df.append(pd.DataFrame(dict1)))

    #return())
	#dict1 = {'table': [p_table_name], 'column': [p_column_name], 'validation':p_validation, 'sqlstmt':p_sqlstmt,'result': [p_result], 'comments': [p_comments],'source_value':p_source_value,'target_value':p_target_value}


