import sysconfig
import psycopg2
import time

from enterprise.db_generic import *
from enterprise.credentials import *


def red_plug_me_in():
    if sysconfig.get_platform() == "win-amd64":
        redshift_host = db_windows_host
        redshift_port = db_windows_port
    else:
        redshift_host = db_lambda_host
        redshift_port = db_lambda_port

    try:
        conn_redshift = psycopg2.connect(
            "dbname=" + redshift_database + " user=" + redshift_user + " password=" + redshift_password + " port=" + redshift_port + " host=" + redshift_host)
        print('Connection to redshift:',' sucessfully created', time.ctime())
        return (conn_redshift)
    except:
        print('Connection to redshift', ' ***ERROR***', time.ctime())


def red_list_pop_tables(p_conn_red,p_schema):

    try:
        v_sql_list_pop="    select tinf.schema as table_schema,tinf.table as table_name,tbl_rows \
                        from  svv_table_info tinf \
                        where tinf.schema in " + "(\'" + p_schema + "\')"

        df_list_pop =  pd.read_sql(v_sql_list_pop, con=p_conn_red) #user svc_integration needs permissions to

        print('List of populated tables created', ' sucessfully', " length= ", len(df_list_pop),time.ctime())
        return(df_list_pop)
    except:
        print('List of populated tables', ' ***ERROR***', time.ctime())
        return(None) ##empty list


def red_list_empty_tables(p_conn_red,p_schema='gasquest2019'):
    #copy-paste1
    v_sql_list_all="\
    select ist.table_schema,ist.table_name \
    from information_schema.tables ist \
    where ist.table_schema in " + "(\'" + p_schema + "\')" +  " and ist.table_type = 'BASE TABLE' "


    df_list_all = pd.read_sql(v_sql_list_all, con=p_conn_red)
    df_list_pop = red_list_pop_tables(p_conn_red=p_conn_red,p_schema=p_schema)

    list_all=df_list_all['table_name'].values.tolist()
    list_pop=df_list_pop['table_name'].values.tolist()

    #--list1-list2 gives me all the tables that need to be exported
    list_pending=(list(set(list_all) - set(list_pop)))
    list_pending.sort() #sorts the list in alphatbetical order

    print('Number of all tables:',len(list_all))
    print('Number of pending tables:',len(list_pending))

    return(list_pending)




def red_run_copy_command(red_cursor,p_schema, p_table,p_mode='print'):

    #needed when prefix dbo in table bamev_table_name_mssql = p_table_with_schema[4:] #eliminates the 3 charts that say dbo
    #The file being exported only has the table name in the file name.
    #The targer (Destination) can be multiple schemas, depending on the environment, so we need to adapt that. A parameter
    #will be used to customize this portion. Also, the file should be saved under Redshift/schema
   # try:
    print('function:red_run_copy_command')
    v_rowcount_redshift=get_row_count_in_db_cur(red_cursor,p_schema=p_schema, p_table=p_table)

    if v_rowcount_redshift>0:
        #print('Data already there, need to truncate')
        return 0
    elif v_rowcount_redshift==0:
        #print('Table is emtpy, ok to load')
        v_redshift_command="copy    p_schema.p_table    from  \'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/p_schema/p_table.txt.gz\' iam_role \'arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev\'     gzip     DELIMITER  \'|\' ESCAPE ACCEPTANYDATE NULL as '\\0\'"
        v_redshift_command=v_redshift_command.replace("p_table",p_table)
        v_redshift_command=v_redshift_command.replace("p_schema", p_schema)

        if p_mode=='execute':
            print('INSIDE EXECUTION',p_table)
            red_cursor.execute(v_redshift_command)
            v_rowcount_redshift_now=get_row_count_in_db_cur(red_cursor,p_schema=p_schema, p_table=p_table) #run again but now AFTER the statement has been executed
            print('ROWCOUNT before:',v_rowcount_redshift,'ROWCOUNT NOW:',v_rowcount_redshift_now)

        #it will print the command in all cases (print only mode and execute mode
        print(p_schema,p_table,p_mode,'command:',v_redshift_command)

def red_run_list_copy_command(p_list='empty', p_mode='execute', target_schema='gasquest2019'):
#The parameter target_schema allows to change the target schema where the data is being loaded to.

    if p_list=='empty':
        list_tables = red_list_empty_tables(target_schema)
    else:
        list_tables=[] #pending add list all tables

    conn_redshift=red_plug_me_in()
    cur = conn_redshift.cursor()
    for x_table in list_tables:
            try:
                red_run_copy_command(cur, target_schema,x_table, p_mode)
                #print('load finished for ', x_table)
            except:
                print(' ')#print('Error loading from s3', x_table)

def temp_print_emtpy():

    list_tables = red_list_empty_tables(p_schema='gasquest2019')
    for x_table in list_tables:
            v_stmt="insert into  #tmp_table_count select   count(*) count_table, 'x_table' as table_name from  x_table"
            v_stmt =v_stmt.replace('x_table',x_table)
            print(v_stmt)

    #used to drop the savetimestamp alter table ' ', 'drop column savetimestamp;'

