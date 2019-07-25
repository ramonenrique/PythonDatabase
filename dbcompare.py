#AUTHOR: RAMON SALAZAR
# FUNCTION: Connect to two databases (postgres sql and aurora MYSQL) to compare tables and databases
# CREATED ON : 2019-06-/AUTHORfrom pg import DB
# 444

import pymysql #AURORA MYSQL
import psycopg2 #REDSHIFT PostGreSQL
import pymssql #MICROSOFT SQL

import pandas as pd
import numpy as np


#Other minor libaries
import time
import sysconfig

#Attempt to resolve this error:ProgrammingError: can't adapt type 'numpy.int64'

# import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

#Returns 1 is data is passed, else -1,-2,-3,-4,-5 depending on the error.


##****************************************************************************************
#
#                                   global variables for connection
#
##****************************************************************************************
#****************************************************************************************

# Variables for storing results into the database
#
# val_line_id 	= 1 #Sets to start at 1, if multiple calls then increment by 1
# val_source_connection_string 	=	"empty"
# val_target_connection_string 	=	"empty"
# val_source_table 	=	"empty"
# val_target_table 	=	"empty"
# val_platform 	=	"empty"

# #Result related
# val_result 	=	"--"
# val_source_rowcount 	=	-1
# val_target_rowcount 	=	-1
# val_check1_table_structure	=	"--"
# val_check2_rowcount 	=	"--"
# val_check3_summary 	=	"--"
# val_check4_str_avg_len 	=	"--"
# val_check5_full_data 	=	"--"
# val_run_results 	=	"--"
# val_other	=	"--"

##############################################################################3
#AURORA
aurora_host='127.0.0.1'
aurora_user='admin'
aurora_port=3309
aurora_passwd='MoosoBFWZ8BScw'
aurora_db='zintegrationtest'

##############################################################################3
#MICROSOFT SQL SERVER
mssql_server = "ec2-34-226-93-157.compute-1.amazonaws.com"
mssql_user = "rsalazar"
mssql_password = "KU4zvmHC"
mssql_db = "GasQuest_2018"

#MICROSOFT SQL SERVER2
mssql_server2 = "legacy-db.bwpmlp.org"
mssql_user2 = "rsalazar"
mssql_password2 = "hfQFQ5am"
mssql_db2 = "GasQuest"
mssql_port2 = "1505"


##############################################################################3
#REDSHIFT
redshift_database = "bwpgmsdev"
redshift_user = "svc_integration"
redshift_password = "!INtbW3PtXGgMS"

db_lambda_port = "5439"
db_lambda_host = "bwp-gms-data-dev.cwkyrxg2o0p1.us-east-1.redshift.amazonaws.com"
db_windows_port = "3310"
db_windows_host = "127.0.0.1"

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
conn_mssql =  pymssql.connect(mssql_server, mssql_user, mssql_password, mssql_db)
conn_mssql2 = pymssql.connect(server=mssql_server2, user=mssql_user2, password=mssql_password2, database=mssql_db2, port=mssql_port2)

audit_row_dict={}

def initialize_dic_control_row():

	dict_control_row={}

	# Table identifiers
	dict_control_row.update({'val_batch_id': -1})
	dict_control_row.update({'val_line_id': -1})

	dict_control_row.update({'val_source_connection_string': "empty"})
	dict_control_row.update({'val_target_connection_string' 	:	"empty"})
	dict_control_row.update({'val_platform' 	:	sysconfig.get_platform() + " python version:" + sysconfig.get_python_version()})
	dict_control_row.update({'val_source_table' :	"empty"})
	dict_control_row.update ({'val_target_table' :	"empty"})

	# Result related
	dict_control_row.update({'val_result' :"--"})
	dict_control_row.update({'val_source_rowcount': 9999999999})
	dict_control_row.update({'val_target_rowcount': 9999999999})
	dict_control_row.update({'val_check1_table_structure': "--"})
	dict_control_row.update({'val_check2_rowcount': "--"})
	dict_control_row.update({'val_check3_summary': "--"})
	dict_control_row.update({'val_check4_str_avg_len': "--"})
	dict_control_row.update({'val_check5_full_data': "--"})
	dict_control_row.update({'val_run_results': "--"})
	dict_control_row.update({'val_other': "--"})

	return dict_control_row


##****************************************************************************************
#
#                                   USING PANDAS
#
##****************************************************************************************

def get_next_batch_id():
	#It does not return a value, changes the dictionary in place

	global audit_row_dict

	sqlstmt='select 	max(batch_id) as curr_batch_id	from integration.validation_control_detail'
	panda_df = pd.read_sql(sqlstmt, con=conn_redshift)
	if panda_df.shape[0]==1: #query run and returned exactly 1 row
		newval=(panda_df['curr_batch_id'][0])+1
		print(newval)
	else:
		#else means there was an error, or this is the first row ever, return 1
		newval=1

	audit_row_dict['val_batch_id'] = newval


def read_table_complete(conn_database,v_table_name):
	sqlstmt1='select * from ' + v_table_name #Concatenating the table name
	panda_df = pd.read_sql(sqlstmt1, con=conn_database)
	if panda_df.shape[0]>0:
		print(v_table_name,'I got data ROWCOUNT:',panda_df.shape[0])
	else:
		print(v_table_name, 'did not work - ROWCOUNT:',panda_df.shape[0])

	return panda_df

def read_table_top_n(p_conn_database,p_table_name, p_top_n_rows):

	#If the connection is a postgres database then the clause needs to have a LIMIT 1000
	#In all other cases, the top(x) will be used (sql server, aurora)

	try:
		if p_conn_database.info.dsn_parameters['krbsrvname']=='postgres':
			sqlstmt1='select * from {0} LIMIT {1}'.format(p_table_name,p_top_n_rows)
			sqlstmt1 = 'select * from ' + p_table_name  + ' LIMIT ' + p_top_n_rows.__str__()
		else:
			#sqlstmt1 = 'select top({0)} from  {1}'.format(p_top_n_rows,p_table_name)
			sqlstmt1 = 'select ' + 'top(' + p_top_n_rows.__str__() + ')' + ' * from ' + p_table_name
	except:
		#sqlstmt1='select top({0)} from {1}'.format(p_top_n_rows, p_table_name)
		sqlstmt1 = 'select ' + 'top(' + p_top_n_rows.__str__() + ')' + ' * from ' + p_table_name

	print('Loading top X rows into panda dataframe using the folloiwng SQL statement')
	print('------>',sqlstmt1)
	panda_df = pd.read_sql(sqlstmt1, con=p_conn_database)

	if panda_df.shape[0]>0:
		print(p_table_name,'Dataframe loaded:',panda_df.shape[0])
	else:
		print(p_table_name, 'Error reading dataframe')

	return panda_df
#************MODIFICATIONS START HERE ************8


def get_row_count_pandas(df):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# USes pandas to get the rowcount. Simply uses the attribute of the dataframe with the number of records loaded
	return df.shape[0]


def valid1_table_structure(df_src,df_tgt):
	# AUTHOR: RAMON SALAZAR
	# Compare the list of columns definition using pandas dataframe.
	# For small tables, the whole dataframe will be used
	# For larget tables, a sample of the table will be send out.
	# RETURNS 1 if validation passed, a negative number if validation failed

	global audit_row_dict

	list_columns_source = []
	list_columns_target = []

	# NOTE: The columns in target are in lowercase, and that may not match the case in source,
	# thus the source columns will be converted to lowercase so the comparison can be done
	for item in df_src.columns: list_columns_source.append(item.lower())
	for item in df_tgt.columns: list_columns_target.append(item.lower())

	df_standarized_src = df_src
	df_standarized_src.columns = list_columns_source  # updating to lowercase

	# The target environment sometimes adds audit columns to the end.
	# This program assumes that is the case, and it will eliminate the extra columns in the target dataframe
	# If the source table has X columns, then X columns will be compared, not more.
	# PENDING this probably can be done differently/shorter
	df_standarized_tgt = df_tgt
	df_standarized_tgt.columns = list_columns_target  # convert to lowercase (even if redundant)
	vlen = min(len(list_columns_source),len(list_columns_target))
	#Using the shortest number of columns. Depending if the solution is integration or migration_as_is
	# The numbers of columns will vary
	df_standarized_src = df_standarized_src.iloc[:, :vlen]
	df_standarized_tgt = df_standarized_tgt.iloc[:,:vlen]


	#Converting the list of columns to string
	# str_columns_source = ''
	# str_columns_target = ''
	# for item in df_standarized_src.columns: str_columns_source += '[' + item + '] '
	# for item in df_standarized_tgt.columns: str_columns_target += '[' + item + '] '

	str_columns_source="-".join(df_standarized_src.columns)
	str_columns_target="-".join(df_standarized_tgt.columns)

	str_columns_target.replace('savetimestamp','') #Custom remove column savetimestamp DELETED manually from target table

	print('SOURCE COLUMNS:',str_columns_source)
	print('TARGET COLUMNS:',str_columns_target)


	if str_columns_source == str_columns_target:
		print('Validation 1 passed: Tables structure (field names ) matches', 'Number of fields:', vlen, '@time:',
			  time.ctime())
		print('List of fields:', str_columns_source)
		audit_row_dict['val_check1_table_structure'] = 'passed'
		return 1
	else:
		print('VALIDATION 1 failed: Table structure is different', str_columns_source, '\n', str_columns_target,
			  '@time:', time.ctime())
		audit_row_dict['val_check1_table_structure']= 'failed'
		audit_row_dict['val_result'] = 'failed'
		return -1

def compare_check(df_src,df_tgt):
 	#Working for integration as of 2019-07-02
	# global val_source_connection_string
	# global val_line_id
	# global val_source_table
	# global val_target_table
	# global val_platform
	#
	# # Result related
	# global val_result
	# global val_source_rowcount
	# global val_target_rowcount
	# global val_check1_table_structure
	# global val_check2_rowcount
	# global val_check3_summary
	# global val_check4_str_avg_len
	# global val_check5_full_data
	# global val_run_results
	# global val_other

	global audit_row_dict

	print('Comparison function-STARTED:','@time:',time.ctime())

	############################################################################################
	#
	#								PASS2:Table Structure
	#
	############################################################################################
	# Because there may be difference in the Cases for source and targer systems, the column
	# names will be standardized to have both lowercase

	list_columns_aurora = []
	list_columns_redshift = []

	# NOTE: The columns in redshift are in lowercase, and that may not match the case in Aurora,
	# thus the aurora columns will be converted to lowercase so the comparison can be done
	for item in df_src.columns: list_columns_aurora.append(item.lower())
	for item in df_tgt.columns: list_columns_redshift.append(item.lower())

	df_standarized_src = df_src
	df_standarized_src.columns = list_columns_aurora  # updating to lowercase

	df_standarized_tgt = df_tgt
	df_standarized_tgt.columns = list_columns_redshift  # convert to lowercase (even if redundant)
	vlen = len(list_columns_aurora)
	df_standarized_tgt = df_standarized_tgt.iloc[:,
						 :vlen]  # MAke sure the number of columns is cut to the same length as Aurora

	# convert the array to  string to compare
	# pending can find another way to convert the list to just strings.

	str_columns_aurora = ''
	str_columns_redshift = ''

	for item in df_standarized_src.columns: str_columns_aurora += '[' + item + '] '
	for item in df_standarized_tgt.columns: str_columns_redshift += '[' + item + '] '

	# The DW will have more fields than Aurora, that is because audit columns (DW) have been intnetionally
	# added towards the end. To make sure the list length is the same, the length of the Aurora list is
	# taken and added to redshift.

	if str_columns_aurora == str_columns_redshift:
		print('Validation 1 passed: Tables structure (field names ) matches', 'Number of fields:', vlen, '@time:',
			  time.ctime())
		print('List of fields:', str_columns_aurora)
		audit_row_dict['val_check1_table_structure'] = 'passed'
	else:
		print('VALIDATION 1 failed: Table structure is different', str_columns_aurora, '\n', str_columns_redshift,
			  '@time:', time.ctime())
		audit_row_dict['val_check1_table_structure'] = 'failed'
		audit_row_dict['val_result'] = "failed"
		return -1

	# Test for equality of the whole data set
	############################################################################################
	#
	#								PASS2:RowCount
	#
	############################################################################################

	#PENDING NEED TO SWITCH TO CALL THE ROW_COUNT FUNCTION


	audit_row_dict['val_source_rowcount'] = df_src.shape[0]
	audit_row_dict['val_target_rowcount'] = df_tgt.shape[0]

	print('Count Source/Target:',audit_row_dict['val_source_rowcount'],audit_row_dict['val_target_rowcount'],'@time:',time.ctime())

	if audit_row_dict['val_source_rowcount']==audit_row_dict['val_target_rowcount']:
		print('Validation 2 passed: Tables have the same rowcount-Good start','@time:',time.ctime())

	elif (audit_row_dict['val_source_rowcount']>=0 and audit_row_dict['val_target_rowcount']>=0):
		diff=audit_row_dict['val_source_rowcount']-audit_row_dict['val_target_rowcount']
		print('VALIDATION 2 failed: Difference in Row Count',diff,'@time:',time.ctime())
		audit_row_dict['val_check2_rowcount']="failed"
		audit_row_dict['val_result'] = "failed"
		return -2

############################################################################################
#
#							VALIDATION3: SUMMARY FOR DATA FRAMES.
#
############################################################################################

	df_src_summary=df_standarized_src.describe(include='all')  # Runs statistics for all fields, including unique, top and frequency for strings
	df_tgt_summary=df_standarized_tgt.describe(include='all')

	print('$$$ TEST USING SUMMARY NEW CODE$$$')
	print(df_src_summary)
	print(df_tgt_summary)

	#is the summary the same
	v_equal_data_summary=df_src_summary.equals(df_tgt_summary)

	if df_src_summary.equals(df_tgt_summary):
		print('Validation 3 passed: Summary of tables matches','@time:',time.ctime())
		audit_row_dict['val_check3_summary'] ='passed'
	else:
		print('VALIDATION 3 failed: Summary of tables DOES NOT match','@time:',time.ctime())
		audit_row_dict['val_check3_summary'] = 'failed'

	#comparing the length of all string values
	print('$$$ TEST STRING COLUMNS USING AVG(LEN(COL)) $$$')
	df_src_str=df_standarized_src.select_dtypes(include=('object'))  # Creates a datafrane oly with strings (or non numeric) data types
	df_tgt_str=df_standarized_tgt.select_dtypes(include=('object'))  # Creates a datafrane oly with strings (or non numeric) data types

	#For Source
	list_src_avg_len=[]
	for col in df_src_str.columns:
		series = df_src_str[col]
		col_avg_len = series.str.len().mean()
		list_src_avg_len.append((col, col_avg_len))  # list of tuples with column name and average length
	print('list_src_avg_len created','time:',time.ctime())

	#For target (same code using tgt data frame)
	list_tgt_avg_len = []
	for col in df_tgt_str.columns:
		series = df_tgt_str[col]
		col_avg_len = series.str.len().mean()
		list_tgt_avg_len.append((col, col_avg_len))  # list of tuples with column name and average length
	print('list_src_avg_len created','time:',time.ctime())

	#Check if the lists are the same, that means both tables have the same average length for all the data
	print(list_src_avg_len)
	print(list_tgt_avg_len)

	if list_src_avg_len==list_tgt_avg_len:
		print('Validation 4 passed: Average length of strings passed:','@time:',time.ctime())
		audit_row_dict['val_check4_str_avg_len'] = 'passed'
	else:
		print('VALIDATION 4 failed: Average length of strings failed','@time:',time.ctime())
		audit_row_dict['val_check4_str_avg_len']  = 'failed'
		audit_row_dict['val_result'] = "failed"

############################################################################################
#
#							VALIDATION5:FULL DATA SET
#
############################################################################################

	#The data needs to be sorted out first, and the index needs to be removed
	#Assuming the first column is a Primary key, else the whole table needs to be sorted out (all columns)
	#need to identify the first column of the list
	# The data needs to be sorted out by the same field
	# The index needs to be reset, otherwise the comparison will not work.
	if v_feature_full_data:
		v_first_column=list_columns_aurora[0]
		df_standarized_src=df_standarized_src.sort_values(by=[v_first_column]).reset_index(drop=True)
		df_standarized_tgt=df_standarized_tgt.sort_values(by=[v_first_column]).reset_index(drop=True)

		v_equal_data_set=df_standarized_src.equals(df_standarized_tgt)

		if v_equal_data_set:
			print('Validation x passed: All rows and columns are the same:','@time:',time.ctime())
			audit_row_dict['val_result']="passed"
			audit_row_dict['val_check5_full_data']='passed'
		else:
			print('VALIDATION x failed: Data sets are different. You may want to run a discrepancy test with function dfDiff','@time:',time.ctime())
			audit_row_dict['val_check5_full_data']='failed'
			audit_row_dict['val_result'] = "failed"
			return -5

	#IF this point is reached, then the structure is the same and the data is the same
	print ('FINAL RESULT passed: All validations passed sucessfully','@time:',time.ctime())
	val_result='passed'

	return 1


# def difflong(df_src,df_tgt):
#     #Input: two data frames
#
# 	list_columns_aurora=[]
# 	global audit_row_dict
#
#     #NOTE: The columns in redshift are in lowercase, and that may not match the case in Aurora,
#     #thus the aurora columns will be converted to lowercase so the comparison can be done
# 	for item in df_src.columns:
# 		list_columns_aurora.append(item.lower())
#
# 	varlen=len(list_columns_aurora)
#     df_standarized_src=df_src
#     df_standarized_src.columns=list_columns_aurora #updating to lowercase
#
#     df_standarized_tgt=df_tgt
#     df_standarized_tgt=df_standarized_tgt.iloc[:,:varlen]  #MAke sure the number of columns is cut to the same length as Aurora
#
#     #convert the array to  string to compare
#     str_columns_aurora=''
#     str_columns_redshift=''
#
#     for item in df_standarized_src.columns: str_columns_aurora+= '['+ item +'] '
#     for item in df_standarized_tgt.columns: str_columns_redshift+= '['+ item +'] '
#
#     #The DW will have more fields than Aurora, that is because audit columns (DW) have been intnetionally
#     #added towards the end. To make sure the list length is the same, the length of the Aurora list is
#     #taken and added to redshift.
#     #if str_columns_aurora==str_columns_redshift
#
#     #Custom data preparation1- The time in aurora can be 000, replace with null
#
#     df_standarized_src.replace(to_replace='0000-00-00 00:00:00', value=np.NaN,inplace=True)
#
#     print('Tables structure (field names ) matches', 'Number of fields:',varlen,'@time:',time.ctime())
#     v_first_column = list_columns_aurora[0]
#     df_standarized_src = df_standarized_src.sort_values(by=[v_first_column]).reset_index(drop=True)
#     df_standarized_tgt = df_standarized_tgt.sort_values(by=[v_first_column]).reset_index(drop=True)
#     dfBool = (df_standarized_src != df_standarized_tgt).stack()
#     diff = pd.concat([df_standarized_src.stack()[dfBool], df_standarized_tgt.stack()[dfBool]], axis=1)
#     diff.columns = ["src", "tgt"]
#     #else:
#     #    print('VALIDATION 2 failed: Table structure is different', str_columns_aurora,'\n',str_columns_redshift)
#
#     return diff

def insert_validation_control_detail(conn_redshift,dcr):

#Uses psycog2
#Need to have the batch id from the main program -Use global variable to avoid sending values unnecessarily
#Need to generate a incremental line_id number

# Pass data to fill a query placeholders and let Psycopg perform
# the correct migration_as_is (no more SQL injections!)

#17 values in the insert stmt
	sqlstmt="INSERT into integration.validation_control_detail (batch_id,line_id ,source_connection_string ,source_table ,target_table ,platform ,result ,source_rowcount ,target_rowcount ,check1_table_structure,check2_rowcount,check3_summary ,check4_str_avg_len ,check5_full_data ,run_results ,other) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

	cur = conn_redshift.cursor()
	cur.execute(sqlstmt,(dcr['val_batch_id'],dcr['val_line_id'],dcr['val_source_connection_string'],dcr['val_source_table'] , dcr['val_target_table'], dcr['val_platform'], dcr['val_result'], dcr['val_source_rowcount'], dcr['val_target_rowcount'] ,dcr['val_check1_table_structure'],dcr['val_check2_rowcount'],dcr['val_check3_summary'],dcr['val_check4_str_avg_len'],dcr['val_check5_full_data'],dcr['val_run_results'],dcr['val_other']))


	# Make the changes to the database persistent
	conn_redshift.commit()
	# Close communication with the database
	cur.close()
	#Leave Connection open for future inserts conn_redshift.close()
	curr_line_id=dcr['val_line_id']
	dcr['val_line_id']=curr_line_id+1

def compare_integration(v_source_table,v_target_table):
	#We know the integration test takes place between an Aurora table and a Redshift table,

	global val_source_connection_string
	global val_source_table
	global val_target_table
	global audit_row_dict

	df_src = read_table_complete(conn_aurora, v_source_table)
	df_tgt = read_table_complete(conn_redshift, v_target_table)

	val_source_connection_string = "Server: " + aurora_host + "Port:" + str(aurora_port)
	val_source_table=v_source_table
	val_target_table=v_target_table

	print('*************************************************************************************')
	print('Comparing tables:',v_source_table,' and ',v_target_table,'@time:',time.ctime())
	compare_check(df_src,df_tgt)
	insert_validation_control_detail(conn_redshift)

def compare_migration_as_is(v_source_table,v_target_table):
	#We know the integration test takes place between an Aurora table and a Redshift table,
    #Thus those connections will be used: df_src and df_src are global variables (dataframes)

	global val_source_connection_string
	global val_source_table
	global val_target_table
	global audit_row_dict

	val_result = "failed" #Set to fail as default, only if the end is reached it will be set to pass.


	df_src = read_table_complete(conn_mssql, v_source_table)
	df_tgt = read_table_complete(conn_redshift, v_target_table)
	print('*************************************************************************************')
	print('Comparing tables:',v_source_table,' and ',v_target_table,'@time:',time.ctime())

	val_source_table=v_source_table
	val_target_table=v_target_table

	insert_validation_control_detail(conn_redshift)



def compare_integration_list():
	# A batch refers to the a whole group of table validations that is kicked off at the same time.

	compare_integration('entity.Contract', 'gmsdwstg.entity_contract')
	compare_integration('entity.Country', 'gmsdwstg.entity_country')
	compare_integration('entity.Entity', 'gmsdwstg.entity_entity')
	compare_integration('entity.EntityAddress', 'gmsdwstg.entity_entityaddress')
	compare_integration('entity.StateProvince', 'gmsdwstg.entity_stateprovince')
	compare_integration('location.Line', 'gmsdwstg.location_line')
	compare_integration('location.LineCapacity', 'gmsdwstg.location_linecapacity')
	compare_integration('location.LineOverrideCapacity', 'gmsdwstg.location_lineoverridecapacity')
	compare_integration('location.LineSequence', 'gmsdwstg.location_linesequence')
	compare_integration('location.Location', 'gmsdwstg.location_location')
	compare_integration('location.LocationAttributes', 'gmsdwstg.location_locationattributes')
	compare_integration('location.LocationAttributesMaster', 'gmsdwstg.location_locationattributesmaster')
	compare_integration('location.LocationEntityInfo', 'gmsdwstg.location_locationentityinfo')
	compare_integration('location.LocationEntityInfoMaster', 'gmsdwstg.location_locationentityinfomaster')
	compare_integration('location.LocationGasQualityTemp', 'gmsdwstg.location_locationgasqualitytemp')
	compare_integration('location.LocationMaxPressure', 'gmsdwstg.location_locationmaxpressure')
	compare_integration('location.LocationMinPressure', 'gmsdwstg.location_locationminpressure')
	compare_integration('location.LocationOwner', 'gmsdwstg.location_locationowner')
	compare_integration('location.LocationProperty', 'gmsdwstg.location_locationproperty')
	compare_integration('location.LocationPropertyMaster', 'gmsdwstg.location_locationpropertymaster')
	compare_integration('location.LocationStatus', 'gmsdwstg.location_locationstatus')
	compare_integration('location.PMBalancingLocation', 'gmsdwstg.location_pmbalancinglocation')
	compare_integration('location.PMLine', 'gmsdwstg.location_pmline')
	compare_integration('location.PMLineSequence', 'gmsdwstg.location_pmlinesequence')
	compare_integration('location.PMLineSequenceConnectingLine', 'gmsdwstg.location_pmlinesequenceconnectingline')
	compare_integration('location.PMSequenceBoundZones', 'gmsdwstg.location_pmsequenceboundzones')
	compare_integration('location.PMSequenceLocation', 'gmsdwstg.location_pmsequencelocation')
	compare_integration('location.PMStatus', 'gmsdwstg.location_pmstatus')
	compare_integration('location.PMVersion', 'gmsdwstg.location_pmversion')
	compare_integration('location.Segment', 'gmsdwstg.location_segment')
	compare_integration('location.SegmentLocation', 'gmsdwstg.location_segmentlocation')
	compare_integration('nomination.Confirmation', 'gmsdwstg.nomination_confirmation')
	compare_integration('nomination.ConfirmationDetail', 'gmsdwstg.nomination_confirmationdetail')
	compare_integration('nomination.Cycle', 'gmsdwstg.nomination_cycle')
	compare_integration('nomination.mike_table', 'gmsdwstg.nomination_mike_table')
	compare_integration('nomination.MOCK_FuelRate', 'gmsdwstg.nomination_mock_fuelrate')
	compare_integration('nomination.Nomination', 'gmsdwstg.nomination_nomination')
	compare_integration('nomination.NominationDetail', 'gmsdwstg.nomination_nominationdetail')
	compare_integration('nomination.NominationScheduledQuantity', 'gmsdwstg.nomination_nominationscheduledquantity')
	compare_integration('nomination.NomScheduledQtyReductionReason',
						'gmsdwstg.nomination_nomscheduledqtyreductionreason')
	compare_integration('nomination.recon_commerce', 'gmsdwstg.nomination_recon_commerce')
	compare_integration('nomination.ReductionReason', 'gmsdwstg.nomination_reductionreason')
	compare_integration('nomination.SEOContractLocation', 'gmsdwstg.nomination_seocontractlocation')
	compare_integration('nomination.Status', 'gmsdwstg.nomination_status')
	compare_integration('nomination.zNomination', 'gmsdwstg.nomination_znomination')
	compare_integration('nomination.zNominationDetail', 'gmsdwstg.nomination_znominationdetail')
	compare_integration('nomination.z_MOCK_FuleRate', 'gmsdwstg.nomination_z_mock_fulerate')
	compare_integration('rfs.PrimaryPoints', 'gmsdwstg.rfs_primarypoints')
	compare_integration('rfs.QuantityReferenceType', 'gmsdwstg.rfs_quantityreferencetype')
	compare_integration('rfs.QuantityType', 'gmsdwstg.rfs_quantitytype')
	compare_integration('rfs.RequestPath', 'gmsdwstg.rfs_requestpath')
	compare_integration('rfs.ServiceRequest', 'gmsdwstg.rfs_servicerequest')


##****************************************************************************************
#
#                                   USING DATABASE IN-SQL
#
##****************************************************************************************



def get_row_count_in_db(conn1,p_table_name,p_method):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# Submit a query to the database rather than doing it in PANDAS.
	#Meant for large/huge tables.

	v_sqlcount = 'SELECT count(*) from ' + p_table_name

	print('SQL STAMEMENT FOR COUNT',v_sqlcount)
	cur = conn1.cursor()
	cur.execute(v_sqlcount,)
	rec=cur.fetchone()
	val_rowcount=rec[0]
	#cur.close()pending hold on closing connection since most are global variables, leave open by now
	return val_rowcount


def valid2_rowcount(conn1,p_source_table,conn2,p_target_table,p_method):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Given two numbers compared the rowcount and send results to log file
	# This function was created to make it generic, so both in-db and pandas could use it

	global val_source_rowcount
	global val_target_rowcount
	global audit_row_dict

	val_source_rowcount=-1
	val_target_rowcount=-1

	if p_method=="in_db":
		print("Using in db queries")
		val_source_rowcount = get_row_count_in_db(conn1,p_source_table,p_method)
		val_target_rowcount = get_row_count_in_db(conn2,p_target_table,p_method)
	elif p_method == "pandas":
		#pending implement better so dataframes will not be recreated review
		print("Using Pandas")
		df_src = read_table_complete(conn_aurora, p_source_table)
		df_tgt = read_table_complete(conn_redshift, p_target_table)
		val_source_rowcount = get_row_count_pandas(df_src)
		val_target_rowcount = get_row_count_pandas(df_tgt)

	#Updates
	audit_row_dict['val_source_rowcount'] = val_source_rowcount
	audit_row_dict['val_target_rowcount'] = val_target_rowcount

	print('Count Source/Target:', val_source_rowcount, val_target_rowcount, '@time:', time.ctime())

	if val_source_rowcount == val_target_rowcount and val_source_rowcount>=0:
		print('Validation 2 passed: Tables have the same rowcount-Good start', '@time:', time.ctime())
		audit_row_dict['val_check2_rowcount'] = "passed"
	else:
		diff = val_source_rowcount - val_target_rowcount
		print('VALIDATION 2 failed: Difference in Row Count', diff, '@time:', time.ctime())
		audit_row_dict['val_check2_rowcount'] = "failed"
		audit_row_dict['val_result'] = "failed"
		return -2

def get_sum_col_agg_in_db(conn,p_table_name):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	# For strings (len(str))
	# The processing is doine IN the database. Pandas only used to store result

	#It needs to connect and get a data sample ONLY to get information about the data structure
	df_all=read_table_top_n(conn,p_table_name, 10000)

	df_num = df_all.select_dtypes(np.number)
	print('Dataframe number of columns is:',len(df_num.columns))
	print(df_num.head(1))

	if len(df_num.columns)>=1:

		list_num_cols = df_num.columns.tolist()  # columsn operations returns an index, converting to list.
		# mikeversin str_avg_cols="avg(" + "),avg(".join(list_num_cols2) + ")"
		str_avg_cols_bigint = "avg(cast(" + " as bigint)), avg(cast(".join(list_num_cols) + " as bigint))"
		# pending add column names
		sqlstmt = 'SELECT ' + str_avg_cols_bigint + " FROM " + p_table_name
		print('TRACE519:sqlstatement avg num',sqlstmt)
		cur = conn.cursor()
		#result = cur.execute(sqlstmt)
		#cur.close() PENDING CLOSE LATER
		panda_df_num_avg = pd.read_sql(sqlstmt, conn)
		return(panda_df_num_avg)
	else:
		#RETURN AN EMPTY DATAFRAME
		print("There are no numeric columns read for this table, returning an empty dataframe")
		audit_row_dict['val_run_results']+='There are no numeric columns read for this table, returning an empty dataframe'
		return pd.DataFrame()

def valid3_summary_in_db(conn_src,p_source_table,conn_tgt,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	# For strings (len(str))
	# The processing is doine IN the database. Pandas only used to store result

	aggnum_src=get_sum_col_agg_in_db(conn_src, p_source_table)
	aggnum_tgt=get_sum_col_agg_in_db(conn_tgt, p_target_table)

	#standarize the dataframe headers before comparison
	#Assume redshift headers, since they are all lowercase
	#Pending: best way would be to add the column names to the SQL statement, but this works by now
	aggnum_src.columns=aggnum_tgt.columns

	if aggnum_src.equals(aggnum_tgt):
		print('Validation 3 passed: Average for numeric columns ok', '@time:', time.ctime())
		audit_row_dict['val_check3_summary'] = "passed"
		audit_row_dict['val_result'] = "passed"
		return 1

	elif (val_source_rowcount >= 0 and val_target_rowcount >= 0):
		diff = val_source_rowcount - val_target_rowcount
		print('VALIDATION 3 failed: Average for numeric columns does not match', diff, '@time:', time.ctime())
		audit_row_dict['val_check3_summary'] = "failed"
		audit_row_dict['val_result'] = "failed"
		print('Source numeric dataframe:',aggnum_src)
		print('Target numeric dataframe:',aggnum_tgt)
		return -3


def compare_in_db(connsrc,conntgt,p_source_table, p_target_table):
	# AUTHOR: RAMON SALAZAR
	# The processing will be performed by the database and the results sent back to pyton
	# Thus those connections will be used: df_src and df_src are global variables (dataframes)

	# This applies to all the functions
	global audit_row_dict

	audit_row_dict = initialize_dic_control_row()
	get_next_batch_id()

	global val_source_connection_string
	global val_source_table
	global val_target_table

	audit_row_dict['val_source_table']=p_source_table
	audit_row_dict['val_target_table']=p_target_table


	print('*************************************************************************************')
	print('Comparing tables:', p_source_table, ' and ', p_target_table, '@time:', time.ctime())

	val_source_connection_string = "Server: "
	val_target_connection_string = "Server: "

	df_src = read_table_top_n(connsrc, p_source_table,100000)
	df_tgt = read_table_top_n(conntgt, p_target_table,100000)

	#PENDING standarize send connections or data frames as needed.

	valid1_table_structure(df_src, df_tgt)
	valid2_rowcount(connsrc, p_source_table, conntgt, p_target_table,p_method='in_db')
	valid3_summary_in_db(connsrc, p_source_table, conntgt, p_target_table)
	insert_validation_control_detail(conn_redshift,audit_row_dict) #pending alwAYS going to redshift, may be add option send to JSON.
	print ('compare_in_db FINISHED- See redshift integration.validation_control_detail for results')

def compare_migration_as_is_list1():
	#Pending Need to set a way to write the connection properties dynamically, else just hardcode
	#based on the decision (easier)

	global audit_row_dict

	audit_row_dict['val_source_connection_string'] = "MSSQL Gasquest2018"
	audit_row_dict['val_target_connection_string'] = "Redshift bwpgmsdev"


#table missing	compare_in_db(conn_mssql, conn_redshift, 'dbo.FaxMailAttachment', 'gasquest.faxmailattachment')
#	compare_in_db(conn_mssql, conn_redshift, 'dbo.FileTransport', 'gasquest.filetransport')
#	compare_in_db(conn_mssql, conn_redshift, 'dbo.HourlyMeasurement', 'gasquest.hourlymeasurement')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.NominationScheduledQuantity', 'gasquest.nominationscheduledquantity')
#	compare_in_db(conn_mssql, conn_redshift, 'dbo.FaxMailEmailItem', 'gasquest.faxmailemailitem')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.AllocationTransactionDetail', 'gasquest.allocationtransactiondetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.OperAllocationTransDetail', 'gasquest.operallocationtransdetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.AvailableCapacity', 'gasquest.availablecapacity')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.LocationSetPointandGasCntlRpt',
				  'gasquest.locationsetpointandgascntlrpt')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.AllocationDetail', 'gasquest.allocationdetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.NominationDetailHistory', 'gasquest.nominationdetailhistory')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.OperationalAllocationDetail', 'gasquest.operationalallocationdetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.DiscountLetter', 'gasquest.discountletter')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.Imbalance', 'gasquest.imbalance')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.AuditStatementFactor', 'gasquest.auditstatementfactor')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.FaxMailEmailRecipient', 'gasquest.faxmailemailrecipient')
	compare_in_db(conn_mssql, conn_redshift, 'SlalomMapping.confirmation_detail_fact',
				  'slalommapping.confirmation_detail_fact')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.NominationDetail', 'gasquest.nominationdetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.ConfirmationDetail', 'gasquest.confirmationdetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.BTU', 'gasquest.btu')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.InvoiceTransDetailChargeType',
				  'gasquest.invoicetransdetailchargetype')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.FaxMailMessageQueue', 'gasquest.faxmailmessagequeue')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.NominationENSDetailHistory', 'gasquest.nominationensdetailhistory')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.EDIConfirmationRequest', 'gasquest.ediconfirmationrequest')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.DailyMeasurement', 'gasquest.dailymeasurement')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.ForecastInvoiceTransDtlChrgTyp',
				  'gasquest.forecastinvoicetransdtlchrgtyp')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.InvoiceTransactionDetail', 'gasquest.invoicetransactiondetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.ProcessLog', 'gasquest.processlog')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.ActualOverrunNoNotice', 'gasquest.actualoverrunnonotice')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.DailyFlows', 'gasquest.dailyflows')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.ForecastInvoiceTransDetail', 'gasquest.forecastinvoicetransdetail')
	compare_in_db(conn_mssql, conn_redshift, 'dbo.BalanceStorageDetail', 'gasquest.balancestoragedetail')



#compare_in_db(conn_mssql, conn_redshift, 'dbo.BalanceStorageDetail', 'gasquest.balancestoragedetail')
#compare_migration_as_is_list1()
