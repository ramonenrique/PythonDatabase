#AUTHOR: RAMON SALAZAR
# FUNCTION: Connect to two databases (postgres sql and aurora MYSQL) to compare tables and databases
# CREATED ON : 2019-06-/AUTHORfrom pg import DB
# 444

import pymysql #AURORA MYSQL
import psycopg2 #REDSHIFT PostGreSQL
import pymssql #MICROSOFT SQL
import random

import pandas as pd
import numpy as np

#Other minor libaries
import time
import sysconfig
import numbers

#EXCEL
import openpyxl
import openpyxl
import xlsxwriter

#Attempt to resolve this error:ProgrammingError: can't adapt type 'numpy.int64'

# import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

#Returns 1 is data is passed, else -1,-2,-3,-4,-5 depending on the error.


##############################################################################3
#AURORA
aurora_host='127.0.0.1'
aurora_user='admin'
aurora_port=3309
aurora_passwd='MoosoBFWZ8BScw'
aurora_db='zintegrationtest'


#MICROSOFT SQL SERVER3
mssql_server = "127.0.0.1"
mssql_user = "rsalazar"
mssql_password = "hfQFQ5am"
mssql_db = "GasQuest"
mssql_port = "3311"


v_feature_full_data=False

if	sysconfig.get_platform()=="win-amd64":
	redshift_host=db_windows_host
	redshift_port=db_windows_port
else:
	redshift_host = db_lambda_host
	redshift_port = db_lambda_port

# query = "COPY gmsdwstg.{{table}} FROM 's3://{{bucket}}/{{key}}' CREDENTIALS 'aws_iam_role={{iam_role}}' {{manifest}} gzip JSON 'auto';"
#Establish connections

conn_aurora = pymysql.connect(host=aurora_host, port=aurora_port, user=aurora_user, passwd=aurora_passwd, db=aurora_db)
conn_redshift = psycopg2.connect("dbname=" + redshift_database + " user=" + redshift_user + " password=" + redshift_password + " port=" + redshift_port + " host=" + redshift_host)
conn_mssql = pymssql.connect(server=mssql_server, user=mssql_user, password=mssql_password, database=mssql_db, port=mssql_port)

v_glob_dyn_sqlstmt='sql'

dfoutput=pd.DataFrame(data=None, index=None, columns=('table','column','validation','sqlstmt','result','comments','source_value','target_value'), dtype=None, copy=False)

print('Finished defining global variables and connections')

def save_xls(df,p_file_name):
	suffix = random.random().__str__()[2:6] #adding a random 4 digit number to the file to avoid error when writing if the file is open, and have a differnet version of the file
	f=suffix.join([p_file_name,'.xlsx'])
	with pd.ExcelWriter(f, engine='xlsxwriter') as writer:  # doctest: +SKIP
		df.to_excel(writer, sheet_name='validation')
		writer.save()         # Close the Pandas Excel writer and output the Excel file.

def add_log_row(df,p_table_name,p_column_name,p_validation,p_result,p_comments,p_source_value,p_target_value,p_sqlstmt):
    dict1 = {'table': [p_table_name], 'column': [p_column_name], 'validation':p_validation, 'sqlstmt':p_sqlstmt,'result': [p_result], 'comments': [p_comments],'source_value':p_source_value,'target_value':p_target_value}
    return(df.append(pd.DataFrame(dict1)))


def prepare_sql(conn, p_table_name,p_column_name,p_data_type):
	#Need to customize because the language and functions is different across platforms.
	#The data returned is also different in format, someitmes Aurora adds decimals to the numbers and microsoft only
	#returns integers

	#prerequisite: the table parameter must contain already the schema name gms.v_location for instance
	sqlstmt=""
	#differences
	#microsoft uses len, aurora uses lenth
	#microsoft does not have trim in this version, using rtrim and ltrim, aurora can use trim directly
	#avg(cast(len(col_trim) as decimal))) as string_check2_string_length

	try:
		if p_data_type in ('char', 'varchar'):
			if conn.__str__()[1:8] == 'pymssql': #MICROSOFT SQL SERVER
				sqlstmt = "select count(distinct(col_trim)) as string_check1_count_distinct " \
						  + " ,avg(cast (len(col_trim)  as decimal)) as string_check2_string_length " \
						  + "  from (select ltrim(rtrim(col_x)) as col_trim from table) t " \
						  + "	where col_trim is not null  and  len(col_trim)>1 "
			if conn.__str__()[1:8] == 'pymysql':
				sqlstmt = "select count(distinct(col_trim)) as string_check1_count_distinct " \
							",avg(length(col_trim)) as string_check2_string_length " \
						     + " from (select trim(col_x) as col_trim " \
							 + "	     from table) t " \
						      + " where  col_trim is not null and length(col_trim) > 1 "
		elif p_data_type in ('smallint', 'int', 'bigint'):
			if conn.__str__()[1:8] == 'pymssql': sqlstmt = 'select sum(cast(col_x as bigint)) as check_num_sum from table '  # MICROSOFT
			if conn.__str__()[1:8] == 'pymysql': sqlstmt = 'select sum(col_x) as check_num_sum from table'  # AURORA
		elif p_data_type in ('decimal'):
			#can not convert to bigint because then the decimals are lost, need solution to manage big numbers.
			if conn.__str__()[1:8] == 'pymssql': sqlstmt = 'select sum(col_x) as check_decimal_sum from table '  # MICROSOFT
			if conn.__str__()[1:8] == 'pymysql': sqlstmt = 'select sum(col_x) as check_decimal_sum from table'  # AURORA
		elif p_data_type in ('date', 'datetime'):
			sqlstmt="select min(col_x) as date_check1_min, max(col_x) as date_check2_max, count(col_x) as  date_check3_count_pop, count(distinct col_x) as  date_check4_distinct 	from table "
		else:
			print('which_function:Unrecognized data type')
			sqlstmt = 'unspecified'
	except:
		print('prepare_sql failed ERROR:P_DATA_TYPE', p_data_type)
		sqlstmt = 'unspecified'

	#For all cases, need to replace colum name and table name at the end
	sqlstmt = sqlstmt.replace("table", p_table_name)
	sqlstmt = sqlstmt.replace("col_x", p_column_name)

	return sqlstmt


def get_result_in_db(conn,p_table_name,p_column_name,p_data_type):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	#  executes select avg(len(x) from table in a specified database
	#p_column_name should be a string data type.

	v_sqlstmt=''
	v_sqlstmt = prepare_sql(conn, p_table_name, p_column_name, p_data_type)

	# if p_data_type in ('char', 'varchar','smallint', 'int', 'bigint','decimal'):

	if v_sqlstmt!='unspecified':
		try:
			cur = conn.cursor()
			df_result = pd.read_sql(v_sqlstmt, conn)
			return df_result
		except:
			print('get_result_in_db:ERROR')#RETURN AN EMPTY DATAFRAME
			print('SQLSTMT ERRORED OUT:',v_sqlstmt)
			return pd.DataFrame()
	else:
		print("get_result_in_db:unspecified function-CANT PROCESS")
		return pd.DataFrame()

	# if p_data_type in ('date', 'datetime'):
	# 	#For dates the treatment is a bit different since we can apply at lesat 4 functions min, max, count distinct
	# 	#The same query work for AURORA AND MYSSQL, so no customization is necessary
	# 	try:
	# 		cur = conn.cursor()
	# 		df_result = pd.read_sql(v_sqlstmt, conn)
	# 		#For dates, returning the whole dataframe with the four results
	# 		return(df_result)
	# 	except:
	# 		print(p_table_name, '.', p_column_name, 'FAILED-Error executing sql stmt', 'FAILED')
	# 		return pd.DataFrame()


def compare_col_poly_in_db(conn_src,p_source_table,p_column_name,p_data_type,conn_tgt,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	# The table name in the target database can change. The column name MUST be the same.
	#The list that will be passed to compare_col_poly_in_db needs to be hardcoded
	#because the table names vary
	global dfoutput
	v_save_two_sqlstmt=''

	p_column_name=p_column_name.lower()

	if p_column_name in ('datetimeadded','datetimemodified','dateeffective','dateexpire','addedby','modifiedby'):
		print(p_source_table, '.', p_column_name, 'passed-Audit Column ignored by design')
		return 1

	result_src=get_result_in_db(conn_src, p_source_table, p_column_name, p_data_type)
	result_tgt=get_result_in_db(conn_tgt, p_target_table, p_column_name, p_data_type)

	v_save_two_sqlstmt = 'MICROSOFT SQL:'+prepare_sql(conn_src, p_source_table, p_column_name, p_data_type)
	v_save_two_sqlstmt = v_save_two_sqlstmt+ '\n' + 'AURORA MYSQL:' + prepare_sql(conn_tgt, p_target_table, p_column_name, p_data_type)

	print('supercheck:',v_save_two_sqlstmt)

	# if result_src.empty:
	# 	print(p_source_table, p_column_name, '***FAILED***-EMPTY dataframe returned', 'Probably column not found')
	# 	dfoutput = add_log_row(dfoutput, p_table_name=p_source_table, p_column_name=p_column_name, p_validation=None,p_result='FAILED', p_source_value=None,p_target_value=None,p_comments='if result_src.empty:' + v_glob_save_two_sqlstmt)
	# 	return -1
	#
	# if result_tgt.empty:
	# 	print(p_source_table, '.', p_column_name, '***FAILED***-EMPTY dataframe returned(TARGET)-Probably column not found')
	# 	dfoutput = add_log_row(dfoutput, p_table_name=p_source_table, p_column_name=p_column_name, p_validation=None,p_result='FAILED',  p_source_value=None,p_target_value=None,p_comments='if result_tgt.empty:' + v_glob_save_two_sqlstmt)
	# 	return -1

	#for dates a full dataframe is returned, so we will loop through each of the values and listed them out individually
	list_cols=result_src.columns
	list_src=result_src.values.tolist()
	list_tgt=result_tgt.values.tolist()
	limit_len=len(list_src[0]) ##the result will only have one row ALWAYS, so getting the first row only.

	for i in range(limit_len):
		curr_validation=list_cols[i]
		try:
			value_src=list_src[0][i]
			value_tgt=list_tgt[0][i]
		except:
			value_src=None
			value_tgt=None

	#	print(p_source_table, p_column_name, p_data_type,i,curr_validation)

		# if :
		# 	dfoutput = add_log_row(dfoutput, p_table_name=p_source_table, p_column_name=p_column_name, p_validation=curr_validation, p_result='FAILED', p_source_value=value_src, p_target_value=value_tgt, p_comments='Src value is empty(None)-Can not compare')
		# if :
		# 	dfoutput = add_log_row(dfoutput, p_table_name=p_source_table, p_column_name=p_column_name, p_validation=curr_validation, p_result='FAILED', p_source_value=value_src, p_target_value=value_tgt, p_comments='tgt value is empty(None)-Can not compare')

		#Needs to round up numbers to two decimals before doing the comparison, otherwise it will give a false positive.
		if isinstance(value_src, numbers.Number) and isinstance(value_tgt, numbers.Number):
				value_src=round(value_src, 2)
				value_tgt=round(value_tgt, 2)

		try:
			if isinstance(value_src, type(None)) or isinstance(value_tgt, type(None)):
				v_result = '**FAILED**'
				v_comments = v_save_two_sqlstmt + 'At least one of the values is empty - Can not compare'
			elif value_src==value_tgt:
				v_result = 'passed'
				v_comments = v_save_two_sqlstmt
			else:
				v_result = '**FAILED**'
				v_comments = 'Values are different. See source and target value columns'
		except:
			print('FATAL ERROR')

		#In all cases, write to the log. The variables will control the different results and messages sent to the log
		dfoutput = add_log_row(dfoutput, p_table_name=p_source_table, p_column_name=p_column_name,
							   p_validation=curr_validation, p_result=v_result, p_source_value=value_src,
							   p_target_value=value_tgt, p_comments=v_comments,p_sqlstmt=v_save_two_sqlstmt)

		print(p_source_table, '.', p_column_name, curr_validation, v_result, '(', value_src, '-vs-', value_tgt, ')', v_comments)

	print('End of table/column=========================================================================================')

	v_save_two_sqlstmt='reset'


