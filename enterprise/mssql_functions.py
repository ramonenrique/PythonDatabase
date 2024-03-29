#author: RAMON SALAZAR
# FUNCTION: Keeps all functionst that apply only to MICROSOFT SQL SERVER DATABASE
# HSITORY
# 04-SEP-2019: change the code to use inline queries for BCP export, instead of creating views in the database

import pymssql
import time


#Custom libraries
from error_management import *
from redshift_functions import *


def connect_to_mssql_intentionally_break():
    conn_mssql = pymssql.connect(server=mssql_server, user=mssql_user, password=mssql_password, database=mssql_db, port=mssql_port)
    return(conn_mssql)

def connect_to_mssql_param(mssql_server,mssql_user,mssql_password,mssql_db,mssql_port):

    try:
        conn_mssql = pymssql.connect(server=mssql_server, user=mssql_user, password=mssql_password, database=mssql_db,
                                    port=mssql_port)
        print('connect_to_mssql_param:Connection to MSSQL:',' sucessfully created', time.ctime())
        return (conn_mssql)
    except:
        print('connect_to_mssql_param::Connection to MSSQL:', ' ***ERROR***', time.ctime())
        exception_handler()


def connect_to_mssql_dict(mdic):

    try:
        conn_mssql = pymssql.connect(server=mdic['mssql_server'], user=mdic['mssql_user'], password=mdic['mssql_password'], database=mdic['mssql_database'],port=mdic['mssql_port'])
        print('connect_to_mssql_param:Connection to MSSQL:',' sucessfully created', time.ctime())
        return (conn_mssql)
    except:
        print('connect_to_mssql_param::Connection to MSSQL:', ' ***ERROR***', time.ctime())
        exception_handler()


def mssql_create_query_json(p_conn_mssql,p_schema,p_table_name):

    #TEMPLATE IS create    view  v_export_table as SELECT   f1, f2, f3   from dbo.table
    #cursor=p_conn_mssql.cursor()

    #get me the list of all the columns with the datatype
    try:

        v_sql=f"select  distinct   table_schema,table_name, column_name, ordinal_position, data_type \
                 from INFORMATION_SCHEMA.COLUMNS  \
                 where  table_schema='{p_schema}' and table_name='{p_table_name}'  \
                 order by ordinal_position "

        dfcol = pd.read_sql(v_sql, con=p_conn_mssql)

        lc=[]
        for row_col in dfcol.head(100).itertuples():
            v_original_field=row_col.column_name
            v_lowercase_field=row_col.column_name.lower()

            if row_col.data_type == 'datetime':
                convert_date_col = f"convert(varchar, [{v_original_field}], 21) as [{v_lowercase_field}]"
            else:
                convert_date_col = f"[{v_original_field}] as [{v_lowercase_field}]"

            lc.append(convert_date_col)

            try:
                lc.remove('SaveTimestamp')
            except:
                v_dummy='Field Savetimetamp does not exist in this table, that is ok ' #, p_table_name, ' ***Exception handled ok***', time.ctime())

        v_lc_str=""
        for x in lc:
            v_lc_str=v_lc_str+ "," + (x)

        v_lc_str=v_lc_str[1:5000] #remove the first comma

        v_sql_create_qry = f"SELECT (select  {v_lc_str}  for json path, without_array_wrapper) col_json  from  {p_schema}.{p_table_name}"

        return(v_sql_create_qry) #in case it is needed for referecen or printing
    except:
        exception_handler()

def mssql_export_bcp_json_global_var(p_conn_mssql,p_src_schema, p_table, p_mode='print', p_file_type='json',p_check_exists=1):
    # needed when prefix dbo in table bamev_table_name_mssql = p_table_with_schema[4:] #eliminates the 3 charts that say dbo

    v_source_object_name = p_src_schema + p_table
    # try:
    #     v_rowcount_redshift=get_row_count_in_db(conn_redshift,p_table_with_schema)
    #     if v_rowcount_redshift>=0:'Keep walking'
    # except:
    #     print('This table does not exist in the target - create it or insert data so I can compare')
    #
    #  -C 65001" means UTF8 standard (sets the date in a format that redshift understands)
    #This program was using schema/table before, now just using json qjery[p_src_schema].[p_prefix][p_table]=json now
    global mssql_server
    global mssql_user
    global mssql_db
    global mssql_port
    global mssql_password

    try:
        v_jsonqry=mssql_create_query_json(p_conn_mssql=p_conn_mssql,p_schema=p_src_schema,p_table_name=p_table)
        #print('vjsonquery:',v_jsonqry)

        v_file_name=p_table + "." + p_file_type
        v_file_name=v_file_name.lower()

        v_dos_mssql_bcp_command = "bcp  $v_jsonqry$  queryout  $C:*temp*file_name$  -S mssql_server,mssql_port -U mssql_user -P mssql_password -d mssql_db -c -C 65001"
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("*", '//')
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("$", '"')
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("file_name",v_file_name ) ##file names ideally lowecase

        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("v_jsonqry", v_jsonqry)
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("mssql_server", mssql_server)
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("mssql_user", mssql_user)
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("mssql_db", mssql_db)
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("mssql_port", mssql_port)
        v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("mssql_password", mssql_password)

        path = "C://Program Files//Microsoft SQL Server//Client SDK//ODBC//130//Tools//Binn//" #make sure to use forward slash
        program = "bcp.exe"
        fullpath = '"' + path + program + '"'

        if p_mode == 'print':
            print(v_dos_mssql_bcp_command)
        elif p_mode == 'execute':
            try:
                print("Attempting to export data for", p_table) #adds too much noise:,"command=",v_dos_mssql_bcp_command)
                os.chdir(path)
                #print("DEBUG:Directory set to:", os.getcwd())
                os.system(v_dos_mssql_bcp_command)
                #print('Program [mssql_export_bcp_json sucessfully finished TABLE:',p_table, time.ctime())
            except:
                print('Program [mssql_export_bcp_json:**Error** executing bcp command')
        return (v_dos_mssql_bcp_command)
    except:
        exception_handler()

def mssql_export_bcp_json(p_conn_mssql,p_mssql_conn_dict,p_src_schema, p_table, p_mode, p_exe_path,p_output_folder):

    #print('debug mssql_export_bcp_json READING dictionary and assigning variables ")
    mssql_server=p_mssql_conn_dict["mssql_server"]
    mssql_user = p_mssql_conn_dict["mssql_user"]
    mssql_database = p_mssql_conn_dict["mssql_database"]
    mssql_port = p_mssql_conn_dict["mssql_port"]
    mssql_password = p_mssql_conn_dict["mssql_password"]

    ##print('debug ONLY:',p_mssql_conn_dict)

    try:
        v_jsonqry=mssql_create_query_json(p_conn_mssql=p_conn_mssql,p_schema=p_src_schema,p_table_name=p_table)
    except:
        print('There was an error preparing the json query for: {p_src_schema}.{p_table}')
        exception_handler()
        return(-1)

    #print('debug:preparing bcp command")
    v_file_name=p_table + ".json"
    v_file_name=v_file_name.lower()

    #important note: do not use back slash because THE DOS COMAND WILL ERROR OUT
    v_folder_path=f"{p_output_folder}/{mssql_database}"
    v_full_path=f"{v_folder_path}/{p_table}.json"

    v_dos_mssql_bcp_command_new = f"bcp  *{v_jsonqry}*  queryout  *{v_full_path}*  -S  {mssql_server},{mssql_port} -U {mssql_user} -P {mssql_password} -d {mssql_database} -c -C 65001"
    v_dos_mssql_bcp_command_new =v_dos_mssql_bcp_command_new.replace("*",'"') #the jsonquery and the path must be in double quotes to execute well in dos

    #this is coming from screen v_exe_path = "C://Program Files//Microsoft SQL Server//Client SDK//ODBC//130//Tools//Binn//" #make sure to use forward slash

    #print('debug: Before executing bcp command")
    if p_mode == 'print':
        import random
        #v_random = str(random.randint(1, 101))
        v_print_file_path=f'{p_output_folder}\mssql_bcp_command.txt'
        sourceFile = open(v_print_file_path, 'a+')
        print(f'REM {time.ctime()}Run this command in DOS prompt to export table:{p_table}', file=sourceFile)
        print(v_dos_mssql_bcp_command_new,file=sourceFile)
        print('\n \n',file=sourceFile)
        print('BCP COMMAND written to file:', v_print_file_path)
        sourceFile.close()
    elif p_mode == 'execute':
        try:
            #For execution, needs to replace the single forward slash for DOUBLE, this is the way Python os library needs it.
            v_cmd_dos=v_dos_mssql_bcp_command_new.replace("\\", "/")
            v_exe_path_dos=p_exe_path.replace("\\", "/")
            #print('debug-Attempting to export data for", p_table) #adds too much noise:,"command=",v_dos_mssql_bcp_command)
            #print('debug-Go to where the exe is:", os.getcwd())
            os.chdir(v_exe_path_dos)
            os.system(v_cmd_dos)
            print('mssql_export_bcp_json:Program [mssql_export_bcp_json sucessfully finished TABLE:',p_table, time.ctime())
            return(1)
        except:
            exception_handler()


def mssql_list_tables_to_export(p_conn_mssql,v_source_schema,p_conn_redshift,p_target_schema,p_filter):

    try:
        if p_filter=='all':
            list_tables=db_list_all_tables(p_conn=p_conn_mssql, p_schema=v_source_schema)
        elif p_filter=='pending':
             list_tables = mssql_list_pending_migrate_case_proof(p_conn_mssql=p_conn_mssql, p_source_schema=v_source_schema, p_conn_red=p_conn_redshift, p_target_schema=p_target_schema)
        elif len(p_filter)>2:
            list_tables=mssql_list_filter_tables(p_conn_mssql, v_source_schema,p_filter)
        else:
            print("invalid mode, please specify valid values [all,pending,filter]")
        return(list_tables)
    except:
        exception_handler()


def mssql_bcp_export_schema_json(p_conn_mssql,p_mssql_conn_dict,p_list_tables,p_mode="execute",p_exe_path="unknown",p_output_folder="c://temp"):
    #p_conn_redshift,p_target_schema :these parameters are only needed if you want to check the list of tables across
    #another alternative is to send a parameter with the list of tables to process

    print("mssql_bcp_export_schema_json: # of tables to export:",str(len(p_list_tables)))
    #First step is making sure the results are saved withing a subfolder for that database, since we are managing multiple
    #databses, especially for quantium we have two sets of tables
    
    # For execution, needs to replace the single forward slash for DOUBLE, this is the way Python os library needs it.
    v_folder_path = p_output_folder.replace("\\", "/")
    v_database=p_mssql_conn_dict["mssql_database"]
    
    try:
        os.chdir(p_output_folder)
        os.system(f"mkdir {v_database}")
        print('Folder for database created successfully',v_database,'  in' , p_output_folder)

        for x_table in p_list_tables:
            mssql_export_bcp_json(p_conn_mssql,p_mssql_conn_dict,p_mssql_conn_dict["mssql_schema"], x_table, p_mode, p_exe_path,p_output_folder)

        return(1) #indicates success

    except:
        exception_handler()


def mssql_list_empty_tables(p_conn_mssql,p_schema):

#, s.row_count,SCHEMA_NAME(schema_id  ) schema_name from sys.tables t \
    try:
        v_metadata_mssql = "SELECT t.name as table_name \
                            from sys.tables t JOIN sys.dm_db_partition_stats s \
                            ON t.object_id = s.object_id \
                            AND t.type_desc = 'USER_TABLE' \
                            AND s.index_id IN (0,1) \
                            where s.row_count=0 \
                            and schema_name(schema_id  )='dbo' \
                            order by t.name,s.row_count "

        df_list_zero = pd.read_sql(v_metadata_mssql, con=p_conn_mssql)
        #print('DEBUG:length list zero:',len(df_list_zero))
        return(df_list_zero['table_name'].tolist())
    except:
        exception_handler()


def mssql_list_filter_tables(p_conn_mssql,p_schema,p_like_filter):

#, s.row_count,SCHEMA_NAME(schema_id  ) schema_name from sys.tables t \
    try:
        v_metadata_mssql = f"select TABLE_SCHEMA,TABLE_NAME \
                        from INFORMATION_SCHEMA.TABLES \
                        where TABLE_SCHEMA='{p_schema}' and TABLE_NAME like '%{p_like_filter}%' \
                        ORDER BY TABLE_NAME "


        df_tables = pd.read_sql(v_metadata_mssql, con=p_conn_mssql)
        print('length df_tables:',len(df_tables))
        return(df_tables['TABLE_NAME'].tolist())

    except:
        exception_handler()


def mssql_create_all_export_views_flat_file(conn_mssql,p_target_schema):
#this function is no longer used since we are sending the json qry with the bcp but it could be very useful
#in the future

    try:
        v_metadata_mssql=" select DISTINCT table_name \
                            from INFORMATION_SCHEMA.TABLES \
                            where table_type=*BASE TABLE* \
                            AND TABLE_SCHEMA=*dbo* \
                            order by table_name "

        v_metadata_mssql = v_metadata_mssql.replace("*","'")
        df_list_all = pd.read_sql(v_metadata_mssql, con=conn_mssql)
        #df_tables_only=df_list_all['table_name'].unique()

        #TEMPLATE IS create    view  v_export_table as SELECT   f1, f2, f3   from dbo.table
        cursor=conn_mssql.cursor()

        for row in df_list_all.head(800).itertuples():

            # print('--PROCESSING TABLE:',row.table_name)
            dfcols=pd.read_sql("Select top(1000) * from dbo." + row.table_name,con=conn_mssql)

            lc=dfcols.columns.tolist()

            try:
                lc.remove('SaveTimestamp')
            except:
                v_dummy='keep walking'

            lcstr= str(lc)

            lcstr=  lcstr.replace('[',' ')
            lcstr = lcstr.replace(']',' ')
            lcstr = lcstr.replace("'"," ")

          #  v_sql_create_view = "create view " + p_target_schema + ".v_export_table as SELECT  list_cols  from dbo.table"
            v_sql_create_view = "create    view  SlalomMapping.v_export_table as SELECT  list_cols  from dbo.table"

            v_sql_create_view = v_sql_create_view.replace('table', row.table_name)
            v_sql_create_view = v_sql_create_view.replace('list_cols',lcstr)

            #In the case the view already exists, it needs to be dropped and recreated, otherwise the program will fail
            v_sql_drop_view= " drop view if exists " + p_target_schema +".v_export_table "
            v_sql_drop_view = v_sql_drop_view.replace('table', row.table_name)

            try:
                #cursor.execute(v_sql_drop_view)
                cursor.execute(v_sql_create_view)
                print('Export view for', row.table_name,' sucessfully created', time.time())
            except:
                print('Export view for', row.table_name, ' ***ERROR***', time.time())
                exception_handler()
            #pending submit statement to redshift_database

    except:
        exception_handler()


def mssql_create_export_views_json(p_conn_mssql,p_table_name,p_target_schema='SlalomMapping'):

    #TEMPLATE IS create    view  v_export_table as SELECT   f1, f2, f3   from dbo.table
    cursor=p_conn_mssql.cursor()

    v_sql_cols="Select top(1000) * from dbo." + p_table_name
    dfcols=pd.read_sql("Select top(1000) * from dbo." + p_table_name,con=p_conn_mssql)
    lc = dfcols.columns.tolist()
    ser_dtype = dfcols.dtypes  # this gives me the data type for each one

    template = "convert(varchar, x_field, 21) as x_field"
    for i in range(len(lc)):
        #print(ser_dtype[i])
        if ser_dtype[i] == 'datetime64[ns]':
            convert_date_col = template.replace('x_field', lc[i])
            #print(lc[i], 'despues:', convert_date_col)
            lc[i] = convert_date_col
        else:
            v_dummy = 'just ignore'

    try:
        lc.remove('SaveTimestamp')
    except:
        v_st='Field Savetimetamp does not exist in this table, that is ok '

    lcstr= str(lc)
    lcstr=  lcstr.replace('[',' ')
    lcstr = lcstr.replace(']',' ')
    lcstr = lcstr.replace("'"," ")
    lcstr = lcstr.lower() #this is necessary to convert to redshift

    #v_sql_create_view = "create view " + p_target_schema +".v_export_table_json as select (select  list_cols  for json path, without_array_wrapper) col_json  from dbo.table"

    v_sql_create_view = "create    view  SlalomMapping.v_export_table_json as SELECT  list_cols  from dbo.table"

    #v_sql_create_view = "create view " + p_target_schema + ".v_export_table as SELECT  list_cols  from dbo.table"
    v_sql_create_view = v_sql_create_view.replace('table', p_table_name)
    v_sql_create_view = v_sql_create_view.replace('list_cols',lcstr)

    # In the case the view already exists, it needs to be dropped and recreated, otherwise the program will fail
    v_sql_drop_view = " drop view if exists " + p_target_schema + ".v_export_table_json "
    v_sql_drop_view = v_sql_drop_view.replace('table', p_table_name)

    try:
        cursor.execute(v_sql_drop_view)
        cursor.execute(v_sql_create_view)
        p_conn_mssql.commit()
        print('Export view for', p_target_schema,p_table_name, ' sucessfully created', time.ctime())
    except:
        print('Export view for', p_table_name, ' ***ERROR***', time.ctime())

    return(v_sql_create_view) #in case it is needed for referecen or printing


def mssql_create_export_views_loop_tables(p_conn_mssql, p_target_schema='SlalomMapping'):
    # TO have a valid JSON file, every date field must be converted to string before exporting it
    # Need to read the metadata, create a list of fields, ONLY FOR DATES, need to change using my template.

    v_metadata_mssql=" select DISTINCT table_name \
                        from INFORMATION_SCHEMA.TABLES \
                        where table_type=*BASE TABLE* \
                        AND TABLE_SCHEMA=*dbo* \
                        order by table_name "

    v_metadata_mssql = v_metadata_mssql.replace("*","'")
    df_list_all = pd.read_sql(v_metadata_mssql, con=p_conn_mssql)

    for row in df_list_all.head(1000).itertuples():
        try:
            mssql_create_export_views_json(p_conn_mssql, row.table_name,p_target_schema)
        except:
            v_dummy='if error keep going to the next one'

def mssql_list_all_tables(p_conn_mssql,p_schema):

#, s.row_count,SCHEMA_NAME(schema_id  ) schema_name from sys.tables t \

    try:
        v_metadata_mssql = f"select TABLE_SCHEMA,TABLE_NAME \
                        from INFORMATION_SCHEMA.TABLES \
                        where TABLE_SCHEMA='{p_schema}' \
                        ORDER BY TABLE_NAME "


        df_tables = pd.read_sql(v_metadata_mssql, con=p_conn_mssql)
        print('length df_tables:',len(df_tables))
        return(df_tables['TABLE_NAME'].tolist())
    except:
        exception_handler()

def mssql_list_pending_migrate_case_proof(p_conn_mssql,p_source_schema,p_conn_red,p_target_schema):
    # select TOP 1 TABLE_SCHEMA,TABLE_NAME
    # from INFORMATION_SCHEMA.TABLES
    # where TABLE_SCHEMA='dbo' and lower(TABLE_NAME)='rates_locationsdetails'

    #This function calculates the list of pending tables in SQL SERVER and makes sure it returns the results
    # in the format that SQL SERVER understands, which can be case sensitive

    try:
        list_all_source_case=mssql_list_all_tables(p_conn_mssql,p_source_schema)

        list_pending_in_target = red_list_empty_tables(p_cr=p_conn_red,p_schema=p_target_schema)

        list_pending_source_case=[]
        for tcase in list_all_source_case:
            if tcase.lower() in list_pending_in_target:
                list_pending_source_case.append(tcase)

        list_pending_source_case.sort()
        return(list_pending_source_case)
    except:
        exception_handler()
