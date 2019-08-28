import pandas as pd

def get_row_count_in_db(conn1,p_schema,p_table):
    # AUTHOR: RAMON SALAZAR
    #    DATE: 2019-07-10
    # Submit a query to the database rather than doing it in PANDAS.
    #Meant for large/huge tables.

    v_sqlcount = 'SELECT count(*) from ' + p_schema + "." + p_table

    #print('SQL STAMEMENT FOR COUNT',v_sqlcount)
    cur = conn1.cursor()
    cur.execute(v_sqlcount)
    rec=cur.fetchone()
    val_rowcount=rec[0]
    #cur.close()pending hold on closing connection since most are global variables, leave open by now
    return val_rowcount

def get_row_count_in_db_cur(cur,p_schema,p_table):
    # AUTHOR: RAMON SALAZAR
    #    DATE: 2019-07-10
    # Submit a query to the database rather than doing it in PANDAS.
    #Meant for large/huge tables.

    v_sqlcount = 'SELECT count(*) from ' + p_schema + "." + p_table

    #print('SQL STAMEMENT FOR COUNT',v_sqlcount)
    # cur = conn1.cursor()
    cur.execute(v_sqlcount)
    rec=cur.fetchone()
    val_rowcount=rec[0]
    #cur.close()pending hold on closing connection since most are global variables, leave open by now
    return val_rowcount


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

