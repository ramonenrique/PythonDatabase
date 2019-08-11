#AUTHOR: RAMON SALAZAR
# FUNCTION: Connect to two databases (postgres sql and aurora MYSQL) to compare tables and databases
# CREATED ON : 2019-08-/AUTHORfrom pg import DB
# 			 20190-08-09 added to git version control
#MICROSOFT SQL SERVER3


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

from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


##############################################################################3
mssql_server = "127.0.0.1"
mssql_user = "rsalazar"
mssql_password = "hfQFQ5am"
mssql_db = "GasQuest"
mssql_port = "3311"

#REDSHIFT
redshift_database = "bwpgmsdev"
redshift_user = "svc_integration"
redshift_password = "!INtbW3PtXGgMS"

db_lambda_port = "5439"
db_lambda_host = "bwp-gms-data-dev.cwkyrxg2o0p1.us-east-1.redshift.amazonaws.com"
db_windows_port = "3310"
db_windows_host = "127.0.0.1"


if	sysconfig.get_platform()=="win-amd64":
	redshift_host=db_windows_host
	redshift_port=db_windows_port
else:
	redshift_host = db_lambda_host
	redshift_port = db_lambda_port


v_feature_full_data=False

if	sysconfig.get_platform()=="win-amd64":
	redshift_host=db_windows_host
	redshift_port=db_windows_port
else:
	redshift_host = db_lambda_host
	redshift_port = db_lambda_port

# query = "COPY gmsdwstg.{{table}} FROM 's3://{{bucket}}/{{key}}' CREDENTIALS 'aws_iam_role={{iam_role}}' {{manifest}} gzip JSON 'auto';"
#Establish connections

#conn_aurora = pymysql.connect(host=aurora_host, port=aurora_port, user=aurora_user, passwd=aurora_passwd, db=aurora_db)
conn_redshift = psycopg2.connect("dbname=" + redshift_database + " user=" + redshift_user + " password=" + redshift_password + " port=" + redshift_port + " host=" + redshift_host)
conn_mssql = pymssql.connect(server=mssql_server, user=mssql_user, password=mssql_password, database=mssql_db, port=mssql_port)

v_glob_dyn_sqlstmt='sql'
v_glob_table_name='table'

dfoutput=pd.DataFrame(data=None, index=None, columns=('table','column','validation','sqlstmt','result','comments','source_value','target_value'), dtype=None, copy=False)


print('Finished defining global variables and connections')

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

def add_log_row(df,p_table_name,p_column_name,p_validation,p_result,p_comments,p_source_value,p_target_value,p_sqlstmt):
	#dict1 = {'table': p_table_name, 'column': p_column_name, 'validation':p_validation, 'sqlstmt':p_sqlstmt,'result': p_result, 'comments':p_comments,'source_value':p_source_value,'target_value':p_target_value}
	dict1 = {'table': [p_table_name], 'column': [p_column_name], 'validation':[p_validation], 'sqlstmt':[p_sqlstmt],'result': [p_result], 'comments': [p_comments],'source_value':[p_source_value],'target_value':[p_target_value]}
	return(df.append(pd.DataFrame(dict1)))

    #return())
	#dict1 = {'table': [p_table_name], 'column': [p_column_name], 'validation':p_validation, 'sqlstmt':p_sqlstmt,'result': [p_result], 'comments': [p_comments],'source_value':p_source_value,'target_value':p_target_value}


def list_empty_tables():
    #copy-paste1
    v_sql_list_all="\
    select ist.table_schema,ist.table_name \
    from information_schema.tables ist \
    where ist.table_schema in ('gasquest2019') \
    and ist.table_type = 'BASE TABLE' "

    #--part1 (tables with valid rowcount)
    v_sql_list_pop="\
    select tinf.schema as table_schema,tinf.table as table_name,tbl_rows \
                        from  svv_table_info tinf \
                        where tinf.schema='gasquest2019' "

    df_list_all = pd.read_sql(v_sql_list_all, con=conn_redshift)
    df_list_pop =  pd.read_sql(v_sql_list_pop, con=conn_redshift) #user svc_integration needs permissions to

    list_all=df_list_all['table_name'].values.tolist()
    list_pop=df_list_pop['table_name'].values.tolist()

    #--list1-list2 gives me all the tables that need to be exported
    list_pending=(list(set(list_all) - set(list_pop)))

    print('Number of all tables:',len(list_all))
    print('Number of pending tables:',len(list_pending))

    return(list_pending)


def list_pop_tables():
    #copy-paste1

    #--part1 (tables with valid rowcount)
    v_sql_list_pop="\
    select tinf.schema as table_schema,tinf.table as table_name,tbl_rows \
                        from  svv_table_info tinf \
                        where tinf.schema='gasquest2019' "

    df_list_pop =  pd.read_sql(v_sql_list_pop, con=conn_redshift) #user svc_integration needs permissions to

    return(df_list_pop)


def valid1_table_structure(conn_src,p_source_table,conn_tgt,p_target_table,p_default_rows=100):
	# AUTHOR: RAMON SALAZAR
	# Compare the list of columns definition using pandas dataframe.
	# For small tables, the whole dataframe will be used
	# For larget tables, a sample of the table will be send out.
	# RETURNS 1 if validation passed, a negative number if validation failed

	global dfoutput
	global v_glob_table_name

	df_src = read_table_top_n(conn_src, p_source_table,p_default_rows)
	df_tgt = read_table_top_n(conn_tgt, p_target_table,p_default_rows)

	if df_src.empty or df_tgt.empty:
		v_result = '***FAILED***'
		v_comments = 'Could not access tables'
		print(time.ctime(), v_result, 'COMMENTS:', v_comments)
		print('--------------------------------------------------------------------------------------------------')
		dfoutput = add_log_row(df=dfoutput, p_table_name=p_source_table, p_column_name='TABLE-CHECK',
							   p_validation='Table data structure', p_result=v_result, p_comments=v_comments,
							   p_source_value=None, p_target_value=None, p_sqlstmt=None)
		return('v_result')


	#Dataframes are only used to easily access all the columns and their data types, not to manipulate data.

	l_src_cols_low = []
	l_tgt_cols_low = []

	# NOTE: The columns in target are in lowercase, and that may not match the case in source,
	# thus the source columns will be converted to lowercase so the comparison can be done
	for item in df_src.columns: l_src_cols_low.append(item.lower())
	for item in df_tgt.columns: l_tgt_cols_low.append(item.lower())
	#Remove savetimestamp from the comparison
	#sql server has the savetimestyamp function, but redshift will not have it, this is a customization, it must be removed
	try:
		l_src_cols_low.remove('savetimestamp')  #Custom remove column savetimestamp DELETED manually from target table
		l_tgt_cols_low.remove('savetimestamp')  # Custom remove column savetimestamp DELETED manually from target table
	except:
		print('If savetimestamp coluimn not found, that is ok, i was just making sure to remove it')


	# # The target environment sometimes adds audit columns to the end.
	# 	# This program assumes that is the case, and it will eliminate the extra columns in the target dataframe
	# 	# If the source table has X columns, then X columns will be compared, not more.
	# 	# PENDING this probably can be done differently/shorter

	vlen = min(len(l_src_cols_low),len(l_tgt_cols_low))

	df_normalized_src = df_src.iloc[:,:vlen].copy()
	df_normalized_src.columns = l_src_cols_low[:vlen]  # updating to lowercase

	df_normalized_tgt = df_tgt.iloc[:,:vlen].copy()
	df_normalized_tgt.columns = l_tgt_cols_low[:vlen]  # updating to lowercase

	#Better to give a PASS result only if 100% sure they are equal and have at least 2 values., ELSE considered an error

	if vlen>=2 and np.array_equal(df_normalized_src.columns,df_normalized_tgt.columns):
		v_result='passed'
		print('List of fields:', str(l_src_cols_low))
		v_comments='Colums matching: ' + str(l_src_cols_low)
	else:
		v_result = '***FAILED***'
		missing = set(l_src_cols_low) - set(l_tgt_cols_low)
		v_comments='Colums not found in target:' + str(missing)

	print(time.ctime(),v_result,'COMMENTS:',v_comments)
	print('--------------------------------------------------------------------------------------------------')
	dfoutput=add_log_row(df=dfoutput, p_table_name=p_source_table, p_column_name='TABLE-CHECK', p_validation='Table data structure', p_result=v_result, p_comments=v_comments, p_source_value=None, p_target_value=None, p_sqlstmt=None)
	return (v_result)

#todos estos valores se quedan igual=> use a default if none then use global variables defined at the beginning of the loop?
#better idea?

def get_row_count_in_db(conn1,p_table_name):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# Submit a query to the database rather than doing it in PANDAS.
	#Meant for large/huge tables.

	v_sqlcount = 'SELECT count(*) from ' + p_table_name

	cur = conn1.cursor()
	cur.execute(v_sqlcount,)
	rec=cur.fetchone()
	val_rowcount=rec[0]
	#cur.close()pending hold on closing connection since most are global variables, leave open by now
	return val_rowcount


def valid2_rowcount(conn_src,p_source_table,conn_tgt,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#Gets the rowcount from the two environments/table and compares them.

	global dfoutput

	val_source_rowcount=-1
	val_target_rowcount=-1

	val_source_rowcount = get_row_count_in_db(conn_src, p_source_table)
	val_target_rowcount = get_row_count_in_db(conn_tgt, p_target_table)

	if val_source_rowcount==val_target_rowcount and val_source_rowcount>=1:
		v_result='passed'
		v_comments='Number of rows verified:' + str(val_source_rowcount)
	else:
		v_result = '***FAILED***'
		diff=val_source_rowcount -	val_target_rowcount
		v_comments='Tables differ by X number of rows:' + str(diff)


	print(time.ctime(),v_result,'COMMENTS:',v_comments)
	#print('Attempting to add row to the log dataframe/xls')
	dfoutput=add_log_row(df=dfoutput, p_table_name=p_source_table, p_column_name='TABLE-CHECK', p_validation='2-Table Rowcount', p_result=v_result, p_comments=v_comments, p_source_value=None, p_target_value=None, p_sqlstmt=None)
	print('--------------------------------------------------------------------------------------------------')
	return(v_result)

def database_type(conn):

	v_dbtype = 'UNSPECIFIED'
	try:
		#read from string
		if conn.__str__()[1:8] == 'pymssql': v_dbtype='MICROSOFT_SS'#MICROSOFT SQL SERVER
		if conn.__str__()[1:8] == 'pymysql': v_dbtype='AURORA_MYSQL'
		try:
			#the info property is only part of the connector for redshift, so one must use the
			#try statement, otherwise it will fail when attemptoing to apply to other data types
			if  str(conn.info)[1:9]=='psycopg2': v_dbtype='REDSHIFT_POSTGRES'
		except:
			v_dummy='Connector info property not valid - just ignore'
	except:
		v_dummy = 'Connector info property not valid - just ignore'

	return(v_dbtype)



def valid_col_prepare_sql(p_dbtype,p_table_name,p_column_name,p_data_type):
	#Need to customize because the language and functions is different across platforms.
	#The data returned is also different in format, someitmes Aurora adds decimals to the numbers and microsoft only
	#returns integers

	#prerequisite: the table parameter must contain already the schema name gms.v_location for instance
	#differences
	#microsoft uses len, aurora uses lenth
	#microsoft does not have trim in this version, using rtrim and ltrim, aurora can use trim directly
	#avg(cast(len(col_trim) as decimal))) as string_check2_string_length

 	#which database am I connected to? (microsoft, postgresql or aurora myssql)?
	#The connectors have different properties depending on the libary being used. One must use the try statement
	#to access the properties, otherwise the statement will fail

	sqlstmt="INVALID_SQL"

	try:
		if p_data_type in ('char', 'varchar'):
			#For sql server, need to cast to decimal otherwise the results will be incorrect
			#also eliminating nulls and 1 char strings (empty strings)
			#ltrim(rtrim(col_x)) is done to trim the extra spaces before and after the string
			if p_dbtype=='MICROSOFT_SS':
				sqlstmt = "select count(distinct(col_trim)) as string_check1_count_distinct " \
						  + " ,avg(cast (len(col_trim)  as decimal)) as string_check2_string_length " \
						  + "  from (select ltrim(rtrim(col_x)) as col_trim from table) t " \
						  + "	where col_trim is not null  and  len(col_trim)>1 "
			if p_dbtype in('AURORA_MYSQL','REDSHIFT_POSTGRES'):
				sqlstmt = "select count(distinct(col_trim)) as string_check1_count_distinct " \
							",avg(length(col_trim)) as string_check2_string_length " \
						     + " from (select trim(col_x) as col_trim " \
							 + "	     from table) t " \
						      + " where  col_trim is not null and length(col_trim) > 1 "
		# ***************************NUMBERS ******************************************
		elif p_data_type in ('smallint', 'int', 'bigint'):
			if p_dbtype=='MICROSOFT_SS': sqlstmt = 'select sum(cast(col_x as bigint)) as check_num_sum from table '  # MICROSOFT
			if p_dbtype in('AURORA_MYSQL','REDSHIFT_POSTGRES'):  sqlstmt = 'select sum(col_x) as check_num_sum from table'  # AURORA
		# ***************************NUMBER(DECIMAL) ******************************************
		elif p_data_type in ('decimal'):
			#can not convert to bigint because then the decimals are lost, need solution to manage big numbers.
			#all of them treat the same by now
			sqlstmt = 'select sum(col_x) as check_decimal_sum from table '  # MICROSOFT
		elif p_data_type in ('date', 'datetime'):
			sqlstmt="select min(col_x) as date_check1_min, max(col_x) as date_check2_max, count(col_x) as  date_check3_count_pop, count(distinct col_x) as  date_check4_distinct 	from table "
		else:
			print('which_function:Unrecognized data type')
			sqlstmt = 'INVALID_SQL'
	except:
		print('prepare_sql failed ERROR:P_DATA_TYPE', p_data_type)
		sqlstmt = 'INVALID_SQL'

	#For all cases, need to replace colum name and table name at the end
	sqlstmt = sqlstmt.replace("table", p_table_name)
	sqlstmt = sqlstmt.replace("col_x", p_column_name)

	return sqlstmt


def valid_col_exec_sql(conn,p_table_name,p_column_name,p_data_type):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	#  executes select avg(len(x) from table in a specified database
	#p_column_name should be a string data type.

	v_sqlstmt = valid_col_prepare_sql(database_type(conn), p_table_name, p_column_name, p_data_type)

	if v_sqlstmt!='INVALID_SQL':
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


def val_col_compare(conn_src,p_source_table,p_column_name,p_data_type,conn_tgt,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	# The table name in the target database can change. The column name MUST be the same.
	#The list that will be passed to compare_col_poly_in_db needs to be hardcoded
	#because the table names vary
	global dfoutput
	v_save_two_sqlstmt=''

	v_column_name=p_column_name.lower()


	if v_column_name in ('datetimeadded','datetimemodified','dateeffective','dateexpire','addedby','modifiedby'):
		print(p_source_table, '.', v_column_name, 'passed-Audit Column ignored by design')
		return 1

	# This section is only used to save the executed sql for research purposes.
	# ------------------------------------------------
	v_dbtype1=database_type(conn_src)
	v_dbtype2=database_type(conn_tgt)

	v_save_two_sqlstmt = v_dbtype1 + '=>' + valid_col_prepare_sql(v_dbtype1, p_source_table, v_column_name, p_data_type)
	v_save_two_sqlstmt = v_save_two_sqlstmt+ '\n' + v_dbtype2 + '=>' + valid_col_prepare_sql(v_dbtype2, p_target_table, v_column_name, p_data_type)

	print('supercheck:',v_save_two_sqlstmt)
    #-------------------------------------------------------------

	result_src=valid_col_exec_sql(conn_src, p_source_table, v_column_name, p_data_type)
	result_tgt=valid_col_exec_sql(conn_tgt, p_target_table, v_column_name, p_data_type)

	if result_src.empty or  result_tgt.empty:
		print('Something went wrong with the execution of the SQL, validate, sql connection and retry')
		v_result = '**FAILED**'
		v_comments = 'Values are different. See source and target value columns'
	else:

		#for dates a full dataframe is returned, so we will loop through each of the values and listed them out individually
		list_cols=result_src.columns
		list_src=result_src.values.tolist()
		list_tgt=result_tgt.values.tolist()
		limit_len = len(list_src[0])  ##the result will only have one row ALWAYS, so getting the first row only.

		for i in range(limit_len):
			curr_validation=list_cols[i]
			try:
				value_src=list_src[0][i]
				value_tgt=list_tgt[0][i]
			except:
				value_src=None
				value_tgt=None

			#Needs to round up numbers to two decimals before doing the comparison, otherwise it will give a false positive.
			if isinstance(value_src, numbers.Number) and isinstance(value_tgt, numbers.Number):
					value_src=round(value_src, 2)
					value_tgt=round(value_tgt, 2)

			try:
				if isinstance(value_src, type(None)) or isinstance(value_tgt, type(None)):
					v_result = '**FAILED**'
					v_comments = 'At least one of the values is empty - Can not compare'
				elif value_src==value_tgt:
					v_result = 'passed'
					v_comments ='Values match'
				else:
					v_result = '**FAILED**'
					v_comments = 'Values are different. See source and target value columns'
			except:
				print('FATAL ERROR')

			#In all cases, write to the log. The variables will control the different results and messages sent to the log
			dfoutput = add_log_row(dfoutput, p_table_name=p_source_table, p_column_name=v_column_name,
								   p_validation=curr_validation, p_result=v_result, p_source_value=value_src,
								   p_target_value=value_tgt, p_comments=v_comments,p_sqlstmt=v_save_two_sqlstmt)

			print('TRACE val_col_compare:',p_source_table, v_column_name, curr_validation, v_result, '(', value_src, '-vs-', value_tgt, ')', v_comments)

	print('End of table/column=========================================================================================')

	v_save_two_sqlstmt='reset'


def valid3_table_all_cols(conn_src,p_source_table,conn_tgt,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-08-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	#Loops through the table,get the column names and calls the validation
	#Final result for the table is logged.

	# df_src = read_table_top_n(conn_src, p_source_table, p_top_n_rows=1000)
	# l_src_cols_low = []
	# for col in df_src.columns:
	# 	col=col.lower()

	v_sqlstmt=" select 	distinct column_name, data_type  from information_schema.columns t 	where	t.TABLE_SCHEMA = 'dbo' 	and t.table_name = 'ActualOverrun' "
	v_sqlstmt=v_sqlstmt.replace('ActualOverrun',p_source_table)

	#execute the sql to get the data types for every column
	df_cols = pd.read_sql(v_sqlstmt, con=conn_src)


	for row in df_cols.head().itertuples():

		if row.column_name in ('savetimestamp','datetimeadded', 'datetimemodified', 'dateeffective', 'dateexpire', 'addedby', 'modifiedby'):
			print(p_source_table, '.', row.column_name, 'passed-Audit Column ignored by design')
		else:
			print(row.column_name, row.data_type)
			val_col_compare(conn_src,p_source_table,row.column_name, row.data_type,conn_redshift,p_target_table)

#                        _
#                       | |
#  _ __   __ _ _ __   __| | __ _ ___
# | '_ \ / _` | '_ \ / _` |/ _` / __|
# | |_) | (_| | | | | (_| | (_| \__ \
# | .__/ \__,_|_| |_|\__,_|\__,_|___/
# | |
# |_|


def read_table_top_n(p_conn_database,p_table_name, p_top_n_rows):

	#If the connection is a postgres database then the clause needs to have a LIMIT 1000
	#In all other cases, the top(x) will be used (sql server, aurora)
#		if p_conn_database.info.dsn_parameters['krbsrvname']=='postgres':
	# sqlstmt1='select * from {0} LIMIT {1}'.format(p_table_name,p_top_n_rows)

	try:
		if p_conn_database.__str__()[1:8] == 'pymssql': #MICROSOFT SQL SERVER
			sqlstmt1 = 'select ' + 'top(' + p_top_n_rows.__str__() + ')' + ' * from ' + p_table_name #SQL SERVER
		else:
			sqlstmt1 = 'select * from ' + p_table_name + ' LIMIT ' + p_top_n_rows.__str__()  # POSTGRESQL redshift
	except:
		#sqlstmt1='select top({0)} from {1}'.format(p_top_n_rows, p_table_name)
		print('Error connecting to database-ABORTING')
		print('Read table attempt using SQL:', sqlstmt1)
		return pd.DataFrame()

	#print('Loading top X rows into panda dataframe using the folloiwng SQL statement')
	#print('------>',sqlstmt1)
	try:
		panda_df = pd.read_sql(sqlstmt1, con=p_conn_database)
		#print(p_table_name,'Dataframe loaded:',panda_df.shape[0])
	except:
		print(p_table_name, 'Error reading dataframe')
		return pd.DataFrame()

	return panda_df

