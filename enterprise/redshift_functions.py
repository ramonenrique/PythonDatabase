import sysconfig
import psycopg2
import time

from db_generic import *
#from credentials import *


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

def connect_to_red_param(var_target_host,var_target_port,var_target_username,var_target_password,var_target_database):

    print( "Connecting to:")
    print( "dbname=" + var_target_database)
    print(" user=" + var_target_username)
    print( " password=*********" )
    print( " port=" + str(var_target_port))
    print( " host=" + var_target_host )

    try:
        conn_redshift = psycopg2.connect( "dbname=" + var_target_database  + " user=" + var_target_username  + " password=" + var_target_password  + " port=" + str(var_target_port ) + " host=" + var_target_host )
        conn_redshift.set_session(readonly=False, autocommit=True)
        print('Connect_to_red_param:Connection to redshift:',' sucessfully created', time.ctime())
        return (conn_redshift)
    except:
        print('Connect_to_red_param::Connection to redshift', ' ***ERROR***', time.ctime())
        return None


def red_list_pop_tables(p_conn_red,p_schema):

    try:
        v_sql_list_pop="    select tinf.schema as table_schema,tinf.table as table_name,tbl_rows \
                        from  svv_table_info tinf \
                        where tinf.schema in " + "(\'" + p_schema + "\')"

        df_list_pop =  pd.read_sql(v_sql_list_pop, con=p_conn_red) #user svc_integration needs permissions to

        print('List of populated tables created', ' sucessfully', " length= ", len(df_list_pop),time.ctime())
        return(df_list_pop['table_name'].values.tolist()) #converts from dataframe to list
    except:
        print('List of populated tables', ' ***ERROR***', time.ctime())
        return(None) ##empty list

def red_list_all_tables(p_conn_red, p_schema):

        print("connection: trace:",p_conn_red)
        print("schema trace:",p_schema)

        try:
            v_sql_list_all = " select ist.table_schema,ist.table_name \
                                from information_schema.tables ist \
                                where ist.table_schema in " + "(\'" + p_schema + "\')" + " and ist.table_type = 'BASE TABLE' "

            print("Querying information schema (metadata) to get the list of tables:",v_sql_list_all)
            df_list_all = pd.read_sql(v_sql_list_all, con=p_conn_red)  # user svc_integration needs permissions to

            #print('List of all  tables in schema created', ' sucessfully', " length= ", len(df_list_all), time.ctime())
            df1=df_list_all['table_name']
            df1tolist=df1.tolist()
            df2tolist=df1.values.tolist()

            print('def red_list_all_tables-Number of tables read:',len(df2tolist))
            return (df1tolist)

        except:
            print('List of populated tables', ' ***ERROR***', time.ctime())
            return (None)  ##empty list


def  red_list_empty_tables(p_cr, p_schema):

    #worked outside x = red_list_all_tables(p_conn_red=conn_redshift, p_schema="qptm_gc")
    list_all = red_list_all_tables(p_conn_red=p_cr,p_schema=p_schema)
    # print("length list all tables in schema:",len(list_all))

    list_pop = red_list_pop_tables(p_conn_red=p_cr,p_schema=p_schema)
    # print("length list populated:",len(list_pop))


    #--list1-list2 gives me all the tables that need to be exported
    list_pending=(list(set(list_all) - set(list_pop)))
    list_pending.sort() #sorts the list in alphatbetical order

    print('Number of all tables:',len(list_all))
    print('Number of pending tables:',len(list_pending))

    return(list_pending)


def red_print_sql_empty_count(list_tables):
    #This only generates SQL to be later plugged into a client to get the roucount for empty tables

    v_stmt = "insert into  #tmp_table_count select   count(*) count_table, 'x_table' as table_name from  x_table"
    for x_table in list_tables:
            print(v_stmt.replace('x_table',x_table))




def red_run_copy_command(conn_redshift,p_schema, p_table,p_mode='print'):

    #needed when prefix dbo in table bamev_table_name_mssql = p_table_with_schema[4:] #eliminates the 3 charts that say dbo
    #The file being exported only has the table name in the file name.
    #The targer (Destination) can be multiple schemas, depending on the environment, so we need to adapt that. A parameter
    #will be used to customize this portion. Also, the file should be saved under Redshift/schema
   # try:
    #this works for flat files, changed to JSON NOW v_redshift_command="copy    p_schema.p_table    from  \'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/p_schema/p_table.txt.gz\' iam_role \'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev\'     gzip     DELIMITER  \'|\' ESCAPE ACCEPTANYDATE NULL as '\\0\'"
    #jSON TEMPALTE IS copy gasquest2019.balanceactivity  from 's3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/gasquest2019/balanceactivity.json.gz'   iam_role 'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev' gzip json 'auto' ;

    v_table=p_table.lower() #Tables in redshift are in lowercase, file name and table name should be in lowercase.

    v_redshift_command = "copy p_schema.p_table        from 's3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/p_schema/p_table.json.gz'   iam_role 'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev' gzip json 'auto' "
    v_redshift_command = v_redshift_command.replace("p_table", v_table)
    v_redshift_command = v_redshift_command.replace("p_schema", p_schema)

    if p_mode == 'print':
        print(v_redshift_command)
    elif p_mode=='execute':
       # print('Execute mode')
        v_rowcount_redshift = db_get_row_count_in_db(conn_redshift, p_schema=p_schema, p_table=p_table)

        if v_rowcount_redshift > 0:
            print('Data already exists in the table:',p_table, 'truncate table first and try again')
            return 0
        elif v_rowcount_redshift == 0:
            #TEMPLATE FOR COPY COMMAND  ="copy    p_schema.p_table    from  \'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/p_schema/p_table.txt.gz\'  iam_role \'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev\'  gzip  json 'auto' "
            cur = conn_redshift.cursor()
            cur.execute(v_redshift_command)
            v_rowcount_redshift_now=db_get_row_count_in_db(conn_redshift,p_schema=p_schema, p_table=v_table) #run again but now AFTER the statement has been executed
            print(f'Program [red_run_copy_command] successfully loaded {p_schema}.{p_table} Rowcount is {v_rowcount_redshift_now}', time.ctime())
            #it will print the command in all cases (print only mode and execute mode
            cur.close()

    #NOTE:The calling environment must handle the commits since it is at connection level not cursor levl.
    #We are going to have multiple tables loaded, there is no need to commit for each one.



def red_run_copy_command_schema(cdict, p_mode='execute', p_filter="all"):
#The parameter target_schema allows to change the target schema where the data is being loaded to.

    #template is  connect_to_red_param(var_target_host,var_target_port,var_target_username,var_target_password,var_target_database):
    conn_redshift=connect_to_red_param(cdict['redshift_host'],cdict['redshift_port'],cdict['redshift_user'],cdict['redshift_password'],cdict['redshift_database'])

    if p_filter=="pending":
        list_tables = red_list_empty_tables(conn_redshift, cdict['redshift_schema'])
    elif p_filter=="all":
        list_tables = red_list_all_tables(conn_redshift,cdict['redshift_schema'])
    elif len(p_filter)>2:
        list_all = red_list_all_tables(conn_redshift, cdict['redshift_schema'])
        list_tables=[]
        for e in list_all:
            if p_filter in e:
                list_tables.append(e)
    else:
        print("Invalid option for filter")
        list_tables = []

    print('Getting ready to run redshift copy command for ', len(list_tables) , ' tables ')
    for x_table in list_tables:
        try:
            red_run_copy_command(conn_redshift, cdict['redshift_schema'].lower(),x_table.lower(), p_mode)
        except:
            print('Error loading from s3', x_table)

    conn_redshift.commit()
    print('Program [red_run_copy_command_schema] sucessfully finished', time.ctime())

# red_run_copy_command(red_cursor,p_schema='gasquest2019', p_table='LocationOverrideCapacity',p_mode='execute')

