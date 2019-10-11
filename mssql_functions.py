import pymssql
from enterprise.inv_credentials import *
from enterprise.redshift_functions import *
import os
import time

def connect_to_mssql():
    conn_mssql = pymssql.connect(server=mssql_server, user=mssql_user, password=mssql_password, database=mssql_db, port=mssql_port)
    return(conn_mssql)

def mssql_create_all_export_views_flat_file(conn_mssql,p_target_schema):

    v_metadata_mssql=" select DISTINCT table_name \
                        from INFORMATION_SCHEMA.tables \
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
        v_sql_cols="Select top(1000) * from dbo." + row.table_name
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

        #pending submit statement to redshift_database



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
        print('Field Savetimetamp does not exist in this table, that is ok ', p_table_name, ' ***Exception handled ok***', time.ctime())

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
        #cursor.execute(v_sql_drop_view)
        cursor.execute(v_sql_create_view)
        print('Export view for', p_target_schema,p_table_name, ' sucessfully created', time.ctime())
    except:
        print('Export view for', p_table_name, ' ***ERROR***', time.ctime())



def mssql_create_export_views_loop_tables(p_conn_mssql, p_target_schema='SlalomMapping'):
    # TO have a valid JSON file, every date field must be converted to string before exporting it
    # Need to read the metadata, create a list of fields, ONLY FOR DATES, need to change using my template.

    v_metadata_mssql=" select DISTINCT table_name \
                        from INFORMATION_SCHEMA.tables \
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




def mssql_export_bcp(p_src_schema, p_table, p_prefix, p_mode='print', p_file_type='json'):
    # needed when prefix dbo in table bamev_table_name_mssql = p_table_with_schema[4:] #eliminates the 3 charts that say dbo

    v_source_object_name = p_src_schema + p_table
    # try:
    #     v_rowcount_redshift=get_row_count_in_db(conn_redshift,p_table_with_schema)
    #     if v_rowcount_redshift>=0:'Keep walking'
    # except:
    #     print('This table does not exist in the target - create it or insert data so I can compare')
    #
    #  -C 65001" means UTF8 standard (sets the date in a format that redshift understands)

    v_dos_mssql_bcp_command = "bcp   [p_src_schema].[p_prefix][p_table]    out    C:*temp*as_is_migration_large_tables*[p_table].[p_file_type] -S 127.0.0.1,3311 -U rsalazar -P hfQFQ5am$ -d Gasquest -c -C 65001"
    v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("[p_src_schema]", p_src_schema)
    v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("[p_table]", p_table)
    v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("[p_prefix]", p_prefix)
    v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("[p_file_type]", p_file_type)

    v_dos_mssql_bcp_command = v_dos_mssql_bcp_command.replace("*", '\\')

    if p_mode == 'print':
        print(v_dos_mssql_bcp_command)
    elif p_mode == 'execute':
        v_dos_chgdir = "Cd C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\\130\Tools\Binn"
        os.system(v_dos_chgdir)
        print(v_dos_mssql_bcp_command)
        os.system(v_dos_mssql_bcp_command)

    return (v_dos_mssql_bcp_command)


def print_bcp_all(p_mode='print'):
    list_pending_tables = red_list_empty_tables()
    list_pending_tables.sort()

    # for x_table in list_pending_tables:
    #     try:
    #         export_mssql_bcp(p_table=x_table, p_src_schema="SlalomMapping", p_prefix="v_export_", p_mode='print')
    #         #temp_rename(x_table)
    #     except:
    #         print(sys.exc_info()[0])
    #         print('Error exporting',x_table)

    for x_table in list_pending_tables:
        mssql_export_bcp(p_table=x_table, p_src_schema="SlalomMapping", p_prefix="v_export_", p_mode=p_mode)


