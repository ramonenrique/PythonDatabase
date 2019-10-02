from enterprise.db_generic import *
from enterprise.excel import *
from enterprise.redshift_functions import  *
from enterprise.mssql_functions import *

import time
import numpy as np
import numbers


def valid1_table_structure(conn_src ,p_source_schema,p_source_table ,conn_tgt ,p_target_schema,p_target_table ,p_default_rows=100):
    # AUTHOR: RAMON SALAZAR
    # Compare the list of columns definition using pandas dataframe.
    # For small tables, the whole dataframe will be used
    # For larget tables, a sample of the table will be send out.
    # RETURNS 1 if validation passed, a negative number if validation failed

    global dfoutput
    global v_glob_table_name

    df_src = db_read_table_top_n(conn_src, p_source_schema, p_source_table ,p_default_rows)
    df_tgt = db_read_table_top_n(conn_tgt, p_target_schema, p_target_table ,p_default_rows)

    if df_src.empty or df_tgt.empty:
        v_result = '***FAILED***'
        v_comments = 'Could not access tables'
        print(time.ctime(), v_result, 'COMMENTS:', v_comments)
        print('--------------------------------------------------------------------------------------------------')
        dfoutput = add_log_row(df=dfoutput, p_table_name=p_source_table, p_column_name='TABLE-CHECK',
                               p_validation='Table data structure', p_result=v_result, p_comments=v_comments,
                               p_source_value=None, p_target_value=None, p_sqlstmt=None)
        return('v_result')

    # Dataframes are only used to easily access all the columns and their data types, not to manipulate data.

    l_src_cols_low = []
    l_tgt_cols_low = []

    # NOTE: The columns in target are in lowercase, and that may not match the case in source,
    # thus the source columns will be converted to lowercase so the comparison can be done
    for item in df_src.columns: l_src_cols_low.append(item.lower())
    for item in df_tgt.columns: l_tgt_cols_low.append(item.lower())
    # Remove savetimestamp from the comparison
    # sql server has the savetimestyamp function, but redshift will not have it, this is a customization, it must be removed
    try:
        l_src_cols_low.remove('savetimestamp')  # Custom remove column savetimestamp DELETED manually from target table
        l_tgt_cols_low.remove('savetimestamp')  # Custom remove column savetimestamp DELETED manually from target table
    except:
        print('If savetimestamp coluimn not found, that is ok, i was just making sure to remove it')


    # # The target environment sometimes adds audit columns to the end.
    # 	# This program assumes that is the case, and it will eliminate the extra columns in the target dataframe
    # 	# If the source table has X columns, then X columns will be compared, not more.
    # 	# PENDING this probably can be done differently/shorter

    vlen = min(len(l_src_cols_low) ,len(l_tgt_cols_low))

    df_normalized_src = df_src.iloc[: ,:vlen].copy()
    df_normalized_src.columns = l_src_cols_low[:vlen]  # updating to lowercase

    df_normalized_tgt = df_tgt.iloc[: ,:vlen].copy()
    df_normalized_tgt.columns = l_tgt_cols_low[:vlen]  # updating to lowercase

    # Better to give a PASS result only if 100% sure they are equal and have at least 2 values., ELSE considered an error

    if vlen>=2 and np.array_equal(df_normalized_src.columns ,df_normalized_tgt.columns):
        v_result ='passed'
        print('List of fields:', str(l_src_cols_low))
        v_comments ='Colums matching: ' + str(l_src_cols_low)
    else:
        v_result = '***FAILED***'
        missing = set(l_src_cols_low) - set(l_tgt_cols_low)
        v_comments ='Colums not found in target:' + str(missing)

    print(time.ctime() ,v_result ,'COMMENTS:' ,v_comments)
    print('--------------------------------------------------------------------------------------------------')
    dfoutput =add_log_row(df=dfoutput, p_table_name=p_source_table, p_column_name='TABLE-CHECK', p_validation='Table data structure', p_result=v_result, p_comments=v_comments, p_source_value=None, p_target_value=None, p_sqlstmt=None)
    return (v_result)


# todos estos valores se quedan igual=> use a default if none then use global variables defined at the beginning of the loop?
# better idea?



def valid2_rowcount(conn_src,p_source_schema,p_source_table,conn_tgt,p_target_schema,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#Gets the rowcount from the two environments/table and compares them.

	global dfoutput

	val_source_rowcount=-1
	val_target_rowcount=-1

	val_source_rowcount = db_get_row_count_in_db(conn_src, p_source_schema,p_source_table)
	val_target_rowcount = db_get_row_count_in_db(conn_tgt, p_target_schema, p_target_table)

	if val_source_rowcount==val_target_rowcount and val_source_rowcount>=0:
		v_result='passed'
		v_comments='Number of rows verified:' + str(val_source_rowcount)
	else:
		v_result = '***FAILED***'
		diff=val_source_rowcount -	val_target_rowcount
		v_comments=f'Source:{str(val_source_rowcount)} Target:{str(val_target_rowcount)} - Diff:' + str(diff)


	print(f'Validation Table {p_target_schema}.{p_target_table} {v_result} COMMENTS: {v_comments}',time.ctime())
	#print('Attempting to add row to the log dataframe/xls')
	dfoutput=add_log_row(df=dfoutput, p_table_name=p_source_table, p_column_name='TABLE-CHECK', p_validation='2-Table Rowcount', p_result=v_result, p_comments=v_comments, p_source_value=None, p_target_value=None, p_sqlstmt=None)
	print('--------------------------------------------------------------------------------------------------')
	return(v_result)


def valid_col_prepare_sql(p_dbtype,p_sch_table_name,p_column_name,p_data_type):
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
						  + " ,avg(cast (len(col_trim)  as decimal(8,5))) as string_check2_string_length " \
						  + "  from (select ltrim(rtrim(lower(col_x))) as col_trim from table) t " \
						  + "	where col_trim is not null  and  len(col_trim)>1 "
			if p_dbtype in('AURORA_MYSQL','REDSHIFT_POSTGRES'):
				sqlstmt = "select count(distinct(col_trim)) as string_check1_count_distinct " \
							",avg(cast (length(col_trim) as decimal(8,5))) as string_check2_string_length " \
						     + " from (select (trim(lower(col_x))) as col_trim " \
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
	sqlstmt = sqlstmt.replace("table", p_sch_table_name)
	sqlstmt = sqlstmt.replace("col_x", p_column_name)

	return sqlstmt


def valid_col_exec_sql(conn,p_sch_table_name,p_column_name,p_data_type):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	#  executes select avg(len(x) from table in a specified database
	#p_column_name should be a string data type.

	v_sqlstmt = valid_col_prepare_sql(db_database_type(conn), p_sch_table_name, p_column_name, p_data_type)

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

def val_col_compare(conn_src,p_source_sch_table,p_column_name,p_data_type,conn_tgt,p_target_sch_table):
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
		print(p_source_sch_table, '.', v_column_name, 'passed-Audit Column ignored by design')
		return 1

	# This section is only used to save the executed sql for research purposes.
	# ------------------------------------------------
	v_dbtype1=db_database_type(conn_src)
	v_dbtype2=db_database_type(conn_tgt)

	v_save_two_sqlstmt = v_dbtype1 + '=>' + valid_col_prepare_sql(v_dbtype1, p_source_sch_table, v_column_name, p_data_type)
	v_save_two_sqlstmt = v_save_two_sqlstmt+ '\n' + v_dbtype2 + '=>' + valid_col_prepare_sql(v_dbtype2, p_target_sch_table, v_column_name, p_data_type)

	print('supercheck:',v_save_two_sqlstmt)
    #-------------------------------------------------------------

	result_src=valid_col_exec_sql(conn_src, p_source_sch_table, v_column_name, p_data_type)
	result_tgt=valid_col_exec_sql(conn_tgt, p_target_sch_table, v_column_name, p_data_type)

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
			dfoutput = add_log_row(dfoutput, p_table_name=p_source_sch_table, p_column_name=v_column_name,
								   p_validation=curr_validation, p_result=v_result, p_source_value=value_src,
								   p_target_value=value_tgt, p_comments=v_comments,p_sqlstmt=v_save_two_sqlstmt)

			print('TRACE val_col_compare:',p_source_sch_table, v_column_name, curr_validation, v_result, '(', value_src, '-vs-', value_tgt, ')', v_comments)

	print('End of table/column=========================================================================================')

	v_save_two_sqlstmt='reset'

def valid3_table_for_loop_cols(conn_src,p_source_schema,p_source_table,conn_tgt,p_target_schema,p_target_table):
	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-08-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	#Loops through the table,get the column names and calls the validation
	#Final result for the table is logged.



	#This query works for both redshift and mssql, no need to create custom versions
	v_sqlstmt=" select 	distinct column_name, data_type  from information_schema.columns t 	where	t.TABLE_SCHEMA = 'myschema' 	and t.table_name = 'ActualOverrun' "
	v_sqlstmt=v_sqlstmt.replace('ActualOverrun',p_source_table)
	v_sqlstmt = v_sqlstmt.replace('myschema', p_source_schema)

	#execute the sql to get the data types for every column
	df_cols = pd.read_sql(v_sqlstmt, con=conn_src)

	#it is better if the schema name is parametereize so we can run the validations on the schemas needed
	v_src_sch_table=p_source_schema + '.' + p_source_table
	v_tgt_sch_table=p_target_schema + '.' +  p_target_table

	for row in df_cols.head().itertuples():
		if row.column_name in ('savetimestamp','datetimeadded', 'datetimemodified', 'dateeffective', 'dateexpire', 'addedby', 'modifiedby'):
			print(p_source_table, '.', row.column_name, 'passed-Audit Column ignored by design')
		else:
			print(row.column_name, row.data_type)
			val_col_compare(conn_src,v_src_sch_table,row.column_name, row.data_type,conn_tgt,v_tgt_sch_table)


def valid_main_schema_compare(conn_src,p_source_schema,conn_tgt,p_target_schema):
#Receives two db and schema, and it will compare the structure and summary data for all the tables
#depending on the database type, it will get the lsit of tables, then call a comparison with the target schema

	global dfoutput

	df_tables=db_dataframe_all_tables(conn_src, p_source_schema)
	i=0

	for t in df_tables.itertuples():
		v_table_name=t.table_name.lower()
		print('Processing Table',str(i), v_table_name)
		# valid1_table_structure(conn_src, p_source_schema, t.table_name, conn_tgt, p_target_schema, t.table_name, p_default_rows=100)
		# valid2_rowcount(conn_src,p_source_schema,v_table_name,conn_tgt,p_target_schema,p_target_table=v_table_name)
		valid3_table_for_loop_cols(conn_src, p_source_schema, p_source_table=v_table_name, conn_tgt=conn_tgt, p_target_schema=p_target_schema,p_target_table=v_table_name)
		i=i+1

	save_xls(dfoutput, 'valid_main_schema_compare')

def valid_main_schema_rowcount_only(conn_src,p_source_schema,conn_tgt,p_target_schema,p_filter):
#Receives two db and schema, and it will compare the structure and summary data for all the tables
#depending on the database type, it will get the lsit of tables, then call a comparison with the target schema

	global dfoutput

	df_tables=db_dataframe_all_tables(conn_src, p_source_schema)
	i=0

	for t in df_tables.itertuples():
		v_table_red=t.table_name.lower()
		v_table_case=t.table_name
		#Penging handle the case where p_filter is none properly. too many ifs NOW.

		if p_filter==None: v_action='process'
		elif p_filter in (v_table_case): v_action='process'
		else: v_action='skip'

		if v_action=='process':
			print('Processing Table', str(i), v_table_case)
			valid2_rowcount(conn_src,p_source_schema,v_table_case,conn_tgt,p_target_schema,p_target_table=v_table_red)
		i=i+1

	save_xls(dfoutput, 'valid_main_schema_rowcount_compare')


def valid_bus_cus_views(conn_src,p_source_view,conn_tgt,p_target_view,p_column_order_by):

		global dfoutput
		v_comments=''

		# READ DATA FOR SOURCE
		v_sqlstmt='select * from ' + p_source_view
		cur = conn_src.cursor()
		dfsrc = pd.read_sql(v_sqlstmt, conn_src)

		#READ DATA FOR TARGET
		v_sqlstmt='select * from ' + p_target_view
		cur = conn_src.cursor()
		dftgt = pd.read_sql(v_sqlstmt, conn_tgt)

		#Prepare SQL is so simple in this case because it is based on a view stored in the database.
		#all we need to do is simply add select * from schema.table_name to the table name.

		# l_src_cols_low=[]
		# for item in dfsrc.columns: l_src_cols_low.append(item.lower())
		# dfsrc.columns=l_src_cols_low

		dfsrc.columns = dftgt.columns

		dfsrc=dfsrc.sort_values(by=['locationstatusdescription']).reset_index(drop=True)
		dftgt=dftgt.sort_values(by=['locationstatusdescription']).reset_index(drop=True)

		v_equal_data_set=dfsrc.equals(dftgt)

		if v_equal_data_set:
			print('Validation x passed: All rows and columns are the same:','@time:',time.ctime())
			v_result = 'passed'
		else:
			print('VALIDATION x failed: Data sets are different. You may want to run a discrepancy test with function dfDiff','@time:',time.ctime())
			v_result = '***FAILED***'

		print(time.ctime(), v_result, 'COMMENTS:', v_comments)
		# print('Attempting to add row to the log dataframe/xls')
		dfoutput = add_log_row(df=dfoutput, p_table_name=p_source_view, p_column_name='TABLE-CHECK',
							   p_validation='FULL DATA SET', p_result=v_result, p_comments=v_comments,
							   p_source_value=None, p_target_value=None, p_sqlstmt=None)
		print('--------------------------------------------------------------------------------------------------')
		return(v_result)




def temp_fix_count_distinct():

		v_red_template="select 	count(distinct 	trim(comment)) cd	from gasquest2019.locationoverridecapacity where 		locationoverridecapacityid 	between 1 and upper_limit 	and length(ltrim(rtrim(comment))) > 1 "
		v_mssql_template="Select 	count(distinct	ltrim(rtrim(comment))) cd	from gasquest.dbo.locationoverridecapacity 	where 	locationoverridecapacityid 	between 1 and upper_limit 		and len(ltrim(rtrim(comment))) > 1 "

		conmsql = connect_to_mssql()
		print('temp_fix_count_distinct')
		connred = connect_to_red()


		# READ DATA FOR source
		for i in range(2000):
			v_red = v_red_template.replace('upper_limit', str(i))
			v_mssql = v_mssql_template.replace('upper_limit',str(i))

			#cur1 = conmsql.cursor()
			dfsrc = pd.read_sql(v_mssql, conmsql)

			#READ DATA FOR TARGET
			#cur2 = connred.cursor()
			dftgt = pd.read_sql(v_red, connred)

			v_equal_data_set=dfsrc.equals(dftgt)

			if v_equal_data_set:
				print(i,'count distinct same','@time:',time.ctime())
			else:
				print(i, 'I got you','@time:',time.ctime())
				return ('stopp here')

