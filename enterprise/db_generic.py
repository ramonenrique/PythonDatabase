import pandas as pd
import os, sys
import traceback

def exception_handler():
	print('DB GENERIC  ERROR HANDLING')
	exc_type, exc_obj, exc_traceback = sys.exc_info() #this helps split the components right away
	v_msg='\n [Exception Type]:' + str(exc_type)
	v_msg = v_msg + '\n [Error message]:' + exc_obj.args[0]
	v_msg = v_msg + '\n [File Name:]' +  os.path.split(exc_traceback.tb_frame.f_code.co_filename)[1]
	v_msg = v_msg + '\n [Line Number]:' + str(exc_traceback.tb_lineno)
	print(v_msg)
	raise



def db_database_type(conn):

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


def db_get_row_count_in_db(conn1,p_schema,p_table):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# Submit a query to the database rather than doing it in PANDAS.
	# #Meant for large/huge tables.

	try:
		v_sqlcount = f'SELECT count(*) from {p_schema}.{p_table} '
		cur = conn1.cursor()
		cur.execute(v_sqlcount)
		rec = cur.fetchone()
		val_rowcount = rec[0]
		cur.close()
		return(val_rowcount)
	except: #pending one could list exactly the exception type here for this scenario
		print('db_get_row_count_in_db',' there was a problem getting the rowcount for this table',v_sqlcount)
		return(-1)



def db_read_table_top_n(p_conn_database,p_schema,p_table_name, p_top_n_rows):

	#If the connection is a postgres database then the clause needs to have a LIMIT 1000
	#In all other cases, the top(x) will be used (sql server, aurora)
#		if p_conn_database.info.dsn_parameters['krbsrvname']=='postgres':
	# sqlstmt1='select * from {0} LIMIT {1}'.format(p_table_name,p_top_n_rows)

	try:
		if p_conn_database.__str__()[1:8] == 'pymssql': #MICROSOFT SQL SERVER
			sqlstmt1 = 'select ' + 'top(' + p_top_n_rows.__str__() + ')' + ' * from ' + p_schema + '.' + p_table_name #SQL SERVER
		else:
			sqlstmt1 = 'select * from ' + p_schema + '.' +  p_table_name + ' LIMIT ' + p_top_n_rows.__str__()  # POSTGRESQL redshift
	except:
		#sqlstmt1='select top({0)} from {1}'.format(p_top_n_rows, p_table_name)
		print('Error connecting to database-ABORTING','sqlstmt used',sqlstmt1)
		exception_handler()
		#return pd.DataFrame() #returns an empty dataframe

	#print('Loading top X rows into panda dataframe using the folloiwng SQL statement')
	#print('------>',sqlstmt1)
	try:
		panda_df = pd.read_sql(sqlstmt1, con=p_conn_database)
		#print(p_table_name,'Dataframe loaded:',panda_df.shape[0])
	except:
		print(p_table_name, 'db_read_table_top_n:Error reading dataframe')
		#return pd.DataFrame()
		exception_handler()

	return panda_df


def db_list_all_tables(p_conn, p_schema):
#THIS FUNCTION works for both redshift and microsoft sql, so placing it in the generic library too.

	try:
		v_sql_list_all = " select ist.table_schema,ist.table_name \
								from  INFORMATION_SCHEMA.TABLES ist \
								where ist.table_schema in " + "(\'" + p_schema + "\')" + " and ist.table_type = 'BASE TABLE' order by ist.table_name "

		print(v_sql_list_all)
		df_list_all = pd.read_sql(v_sql_list_all, con=p_conn)  # user svc_integration needs permissions to

		print('List of all  tables in schema created', ' sucessfully', " length= ", len(df_list_all))
		list_all = df_list_all['table_name'].values.tolist()
		return(list_all)
	except:
		print('db_list_all_tables:List of populated tables', ' ***ERROR***')
		#return (None)  ##empty list
		exception_handler()


def db_dataframe_all_tables(p_conn, p_schema):
#THIS FUNCTION works for both redshift and microsoft sql, so placing it in the generic library too.

	try:
		v_sql_list_all = " select ist.table_schema,ist.table_name \
								from  INFORMATION_SCHEMA.TABLES ist \
								where ist.table_schema in " + "(\'" + p_schema + "\')" + " and ist.table_type = 'BASE TABLE' order by ist.table_name "

		print(v_sql_list_all)
		df_list_all = pd.read_sql(v_sql_list_all, con=p_conn)  # user svc_integration needs permissions to

		print('List of all  tables in schema created', ' sucessfully', " length= ", len(df_list_all))
		return (df_list_all)
	except:
		print('db_dataframe_all_tables:List of populated tables', ' ***ERROR***')
		exception_handler()


def db_list_all_schemas(p_conn):
#THIS FUNCTION works for both redshift and microsoft sql, so placing it in the generic library too.

	try:
		v_sql_list_schema = " select distinct ist.table_schema as table_schema\
								from  INFORMATION_SCHEMA.TABLES ist \
								where ist.table_type = 'BASE TABLE' order by ist.table_schema "

		df_list_schema = pd.read_sql(v_sql_list_schema, con=p_conn)  # user svc_integration needs permissions to

		list_schema = df_list_schema['table_schema'].values.tolist()
		list_schema.sort()
		print('List of all  Schemas created sucessfully length=', len(list_schema))
		print(list_schema)
		return(list_schema)
	except:
		print('List of populated tables', ' ***ERROR***')
		exception_handler()
		#return (None)  ##empty list
