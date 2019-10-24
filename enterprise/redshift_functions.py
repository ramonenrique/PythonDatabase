import sysconfig
import psycopg2
import time

from db_generic import *
#from credentials import *


def copy_config_defaults():
    copy_config = {
        "s3_bucket": 's3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/'
        ,"iam_role": 'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev'
        ,"options": "gzip  json \'auto\'"
    }
    return(copy_config)


def connect_to_red_intentionally_break():
    global db_lambda_port
    global db_lambda_host
    global db_windows_port
    global db_windows_host

    if sysconfig.get_platform() == "win-amd64":
        redshift_host = db_windows_host
        redshift_port = db_windows_port
    else:
        redshift_host = db_lambda_host
        redshift_port = db_lambda_port

    try:
        conn_redshift = psycopg2.connect(
            "dbname=" + redshift_database + " user=" + redshift_user + " password=" + redshift_password + " port=" + redshift_port + " host=" + redshift_host)
        conn_redshift.set_session(readonly=False, autocommit=True)
        print('Connection to redshift:',' sucessfully created', time.ctime())
        return (conn_redshift)
    except:
        print('Connection to redshift', ' ***ERROR***', time.ctime())
        return(-1)

def connect_to_red_param(var_target_host,var_target_port,var_target_username,var_target_password,var_target_database):

    print( "CONNECTING PARAMETERS:")
    print( "host=" + var_target_host )
    print( "port=" + str(var_target_port))
    print( "dbname=" + var_target_database)
    print( "user=" + var_target_username)
    print( "password=*********" )

    try:
        conn_redshift = psycopg2.connect( "dbname=" + var_target_database  + " user=" + var_target_username  + " password=" + var_target_password  + " port=" + str(var_target_port ) + " host=" + var_target_host )
        conn_redshift.set_session(readonly=False, autocommit=True)
        print('Connect_to_red_param:Connection to redshift:',' sucessfully created', time.ctime())
        return (conn_redshift)
    except:
        print('Connect_to_red_param::Connection to redshift', ' ***ERROR***', time.ctime())
        return None

def connect_to_red_dict(rdict):

    try:
        conn_redshift = psycopg2.connect( "dbname=" + rdict['redshift_database']  + " user=" + rdict['redshift_user']  + " password=" + rdict['redshift_password']  + " port=" + str(rdict['redshift_port'] ) + " host=" + rdict['redshift_host'] )
        conn_redshift.set_session(readonly=False, autocommit=True)
        print('Connect_to_red_param:Connection to redshift:',' sucessfully created', time.ctime())
        return (conn_redshift)
    except:
        print('Connect_to_red_param::Connection to redshift', ' ***ERROR***', time.ctime())
        return -1

def red_list_pop_tables(p_conn_red,p_schema):

    #print('debug list pop table parameters',p_conn_red, p_schema)

    try:
        v_sql_list_pop="    select tinf.schema as table_schema,tinf.table as table_name,tbl_rows \
                        from  svv_table_info tinf \
                        where tinf.schema in " + "(\'" + p_schema + "\')"

        df_list_pop =  pd.read_sql(v_sql_list_pop, con=p_conn_red) #user svc_integration needs permissions to

        print('List of populated tables created', ' sucessfully', " length= ", len(df_list_pop),time.ctime())
        return(df_list_pop['table_name'].values.tolist()) #converts from dataframe to list
    except:
        print('List of populated tables', ' ***ERROR***', time.ctime())
        return(-1) ##empty list

def red_list_all_tables(p_conn_red, p_schema):

        #print("DEBUG:connection: trace:",p_conn_red)
        #print("DEBUG:schema trace:",p_schema)

        try:
            v_sql_list_all = " select ist.table_schema,ist.table_name \
                                from information_schema.tables ist \
                                where ist.table_schema in " + "(\'" + p_schema + "\')" + " and ist.table_type = 'BASE TABLE' \
                                order  by   ist.table_name  "

            print("Querying information schema (metadata) to get the list of tables:",v_sql_list_all)
            df_list_all = pd.read_sql(v_sql_list_all, con=p_conn_red)  # user svc_integration needs permissions to

            #print('List of all  tables in schema created', ' sucessfully', " length= ", len(df_list_all), time.ctime())
            df1=df_list_all['table_name']
            df1tolist=df1.tolist()

            print('def red_list_all_tables-Number of tables read:',len(df1tolist))
            return (df1tolist)

        except:
            print('List of populated tables', ' ***ERROR***', time.ctime())
            return (None)  ##empty list


def red_list_filter_tables(p_conn_red, p_schema,p_filter):

    try:
        v_sql_list = f"select ist.table_name from information_schema.tables ist where ist.table_schema in ('{p_schema}') and ist.table_type = 'BASE TABLE' and ist.table_name like '%{p_filter}%' order  by  ist.table_name"

        print("Querying information schema (metadata) to get the list of tables:", v_sql_list)
        df_list = pd.read_sql(v_sql_list, con=p_conn_red)  # user svc_integration needs permissions to

        # print('List of all  tables in schema created', ' sucessfully', " length= ", len(df_list_all), time.ctime())
        df1 = df_list['table_name']
        df1tolist = df1.tolist()

        return (df1tolist)
    except:
        print('red_list_filter_tables', ' ***ERROR***', time.ctime())
        return (-1)  ##empty list


def  red_list_empty_tables(p_cr, p_schema):

    #worked outside x = red_list_all_tables(p_conn_red=conn_redshift, p_schema="qptm_gc")
    list_all = red_list_all_tables(p_conn_red=p_cr,p_schema=p_schema)
    # print("length list all tables in schema:",len(list_all))

    list_pop = red_list_pop_tables(p_conn_red=p_cr,p_schema=p_schema)
    # print("length list populated:",len(list_pop))

    #--list1-list2 gives me all the tables that need to be exported

    if type(list_all) != type(None) and  type(list_pop) != type(None):

        list_pending=(list(set(list_all) - set(list_pop)))
        list_pending.sort() #sorts the list in alphatbetical order

        print('Number of all tables:',len(list_all))
        print('Number of popualted:',len(list_pop))
        print('Number of pending tables (RESULT):',len(list_pending))
    else:
        return(-1)
    return(list_pending)


def red_print_sql_empty_count(list_tables):
    #This only generates SQL to be later plugged into a client to get the roucount for empty tables

    v_stmt = "insert into  #tmp_table_count select   count(*) count_table, 'x_table' as table_name from  x_table"
    for x_table in list_tables:
            print(v_stmt.replace('x_table',x_table))


def red_run_copy_command(p_conn_redshift, p_schema,p_table,p_ccc,p_run_mode='execute'):

    #needed when prefix dbo in table bamev_table_name_mssql = p_table_with_schema[4:] #eliminates the 3 charts that say dbo
    #The file being exported only has the table name in the file name.
    #The targer (Destination) can be multiple schemas, depending on the environment, so we need to adapt that. A parameter
    #will be used to customize this portion. Also, the file should be saved under Redshift/schema
   # try:
    #this works for flat files, changed to JSON NOW v_redshift_command="copy    p_schema.p_table    from  \'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/p_schema/p_table.txt.gz\' iam_role \'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev\'     gzip     DELIMITER  \'|\' ESCAPE ACCEPTANYDATE NULL as '\\0\'"
    #jSON TEMPALTE IS copy gasquest2019.balanceactivity  from 's3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/gasquest2019/balanceactivity.json.gz'   iam_role 'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev' gzip json 'auto' ;

    #The default iam_role is 'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev'
    # print(f'Function red_run_copy_command p_run_mode={p_run_mode}')
    p_s3_bucket=p_ccc["s3_bucket"]
    p_iam_role=p_ccc["iam_role"]
    p_options=p_ccc["options"]

    #print('Connection:',p_conn_redshift,'--schema:', p_schema,'--table:',p_table,'--s3 bucket:',p_s3_bucket,'--IAM role:',p_iam_role,'--CMD OPTIONS:',p_options,'--RUN MODE:',p_run_mode)

    v_table=p_table.lower() #Tables in redshift are in lowercase, file name and table name should be in lowercase.

    v_full_s3_path=f'{p_s3_bucket}/{p_schema}/{p_table}.json.gz'
    v_full_s3_path_quotes="'" + v_full_s3_path + "'"
    v_full_file_path_quotes = "'" + v_full_s3_path + "'"
    v_iam_role_quotes="'" + p_iam_role + "'"

    v_redshift_command=  f'copy  {p_schema}.{p_table}  from {v_full_s3_path_quotes} iam_role {v_iam_role_quotes} {p_options}'

    if p_run_mode == 'print':
        print(v_redshift_command)
    elif p_run_mode=='execute':
       # print('Execute mode')
        v_rowcount_redshift = db_get_row_count_in_db(p_conn_redshift, p_schema=p_schema, p_table=p_table)

        cur = p_conn_redshift.cursor()
        if v_rowcount_redshift > 0:
            print('Data already exists in the table:',p_table, 'Truncating table first')
            cur.execute(f'truncate table {p_schema}.{p_table}')

        #TEMPLATE FOR COPY COMMAND  ="copy    p_schema.p_table    from  \'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/p_schema/p_table.txt.gz\'  iam_role \'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev\'  gzip  json 'auto' "
        cur.execute(v_redshift_command)
        v_rowcount_redshift_now=db_get_row_count_in_db(p_conn_redshift,p_schema=p_schema, p_table=v_table) #run again but now AFTER the statement has been executed
        print(f'Program [red_run_copy_command] successfully loaded {p_schema}.{p_table} Rowcount is {v_rowcount_redshift_now}', time.ctime())
        #it will print the command in all cases (print only mode and execute mode
        cur.close()
    else:
        print('red_run_copy_command:invalid mode. Accepted values are print/execute')
    #NOTE:The calling environment must handle the commits since it is at connection level not cursor levl.
    #We are going to have multiple tables loaded, there is no need to commit for each one.



def red_run_copy_command_schema(cdict, copy_cmd_cfg,p_run_mode='execute', p_filter="all"):
#The parameter target_schema allows to change the target schema where the data is being loaded to.

    #template is  connect_to_red_param(var_target_host,var_target_port,var_target_username,var_target_password,var_target_database):
    #print(f'DEBUG:red_run_copy_command_schema: Received parameters: - p_run_mode{p_run_mode} - p_filter={p_filter}')
    #print(f'DEBUG:Dictionarty with all properties: copy_cmd_cfg {copy_cmd_cfg}' )


    v_schema = cdict['redshift_schema'].lower()
    conn_redshift=connect_to_red_param(cdict['redshift_host'],cdict['redshift_port'],cdict['redshift_user'],cdict['redshift_password'],cdict['redshift_database'])

    if p_filter=="pending":
        list_tables = red_list_empty_tables(conn_redshift, v_schema)
    elif p_filter=="all":
        list_tables = red_list_all_tables(conn_redshift,v_schema)
    elif len(p_filter)>2:
        #print('DEBUG:red_run_copy_command_schema:red_run_copy_command_schema:trying to filter list of tables:',p_filter)
        list_tables = red_list_filter_tables(conn_redshift, v_schema,p_filter)
    else:
        print("Invalid option for filter")
        list_tables = []

    if list_tables!=-1:
        print('red_run_copy_command_schema:Getting ready to run redshift copy command for ', len(list_tables) , ' tables ')
        for x_table in list_tables:
           # try:
            red_run_copy_command(p_conn_redshift=conn_redshift, p_schema=v_schema,p_table=x_table,p_ccc=copy_cmd_cfg,p_run_mode=p_run_mode)
    else:
        print('red_run_copy_command_schema/call list_tables_functins:Error calculating the list of tables')


    conn_redshift.commit()
    print('Program [red_run_copy_command_schema] sucessfully finished', time.ctime())

# red_run_copy_command(red_cursor,p_schema='gasquest2019', p_table='LocationOverrideCapacity',p_mode='execute')

