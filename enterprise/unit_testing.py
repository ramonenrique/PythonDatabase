from enterprise.credentials import *
from enterprise.excel import *
from enterprise.db_generic import *

from enterprise.aurora_functions import *
from enterprise.mssql_functions import *
from enterprise.redshift_functions import *
from enterprise.validations import *
from enterprise.db_generic import *

from enterprise.across import *

conmsql = connect_to_mssql()
# list_zero=mssql_list_empty_tables(conmsql,p_target_schema='dbo')
# print(list_zero)

# mssql_create_all_export_views_flat_file(conmsql)  # This will create ALL the tables. how about if they already exist?

#createst first time in new schema
#mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='DefaultFuelElection',p_target_schema='SlalomDataLake')

#x=mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='DefaultFuelElection',p_target_schema='SlalomMapping')

#overwrite current definition (should nto error out)
# mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='_DefaultFuelElection',p_schema='SlalomDataLake'

# --               _     _     _  __ _
# --              | |   | |   (_)/ _| |
# --  _ __ ___  __| |___| |__  _| |_| |_
# -- | '__/ _ \/ _` / __| '_ \| |  _| __|
# -- | | |  __/ (_| \__ \ | | | | | | |_
# -- |_|  \___|\__,_|___/_| |_|_|_|  \__|


# # list_pop=red_list_pop_tables(red_conn,'gasquest2019')
# # list_pop=red_list_pop_tables(red_conn,'gmsdw')
#
connred=connect_to_red()

#
# l2=red_list_empty_tables(connred,'gasquest2019')
# # print(list_empty1)
#
# l1=red_list_pop_tables(connred,'gasquest2019')
# print(l2)

# list_empty2=red_list_empty_tables(red_conn,'gmsdw')
#

# list_empty=red_list_empty_tables(red_conn,'gasquest2019')
# red_print_sql_empty_count(list_empty)
#


# lp=mig_list_pending_migrate(conmsql,connred,'dbo','gasquest2019')
# print(lp)

# connred.commit()
#
# red_run_copy_command(red_cursor, p_schema='gasquest2019', p_table='LocationOverrideCapacity', p_mode='execute')
# #red_run_copy_command_for_loop(conn_redshift=connred, p_mode='execute', p_schema='gasquest2019')

# print(database_type(connred))
# print(database_type(conmsql))

# x=db_get_row_count_in_db(connred,'gasquest2019','locationoverridecapacity')
# print(x)
# y=db_get_row_count_in_db(conmsql,'dbo','locationoverridecapacity')
# print(y)
#
# df1=db_read_table_top_n(conmsql,'dbo','locationoverridecapacity',100)
# print(df1.head(5))


#valid1_table_structure(conn_src ,p_source_table ,conn_tgt ,p_target_table ,p_default_rows=100):

# result=valid1_table_structure(conn_src=conmsql ,p_source_schema='dbo',p_source_table='locationoverridecapacity',conn_tgt=connred ,p_target_schema='gasquest2019',p_target_table='locationoverridecapacity' )
#
# result=valid1_table_structure(conn_src=conmsql ,p_source_schema='dbo',p_source_table='locationoverridecapacity',conn_tgt=connred ,p_target_schema='gasquest2019',p_target_table='balanceactivity' )

#result=valid2_rowcount(conn_src=conmsql ,p_source_schema='dbo',p_source_table='locationoverridecapacity',conn_tgt=connred ,p_target_schema='gasquest2019',p_target_table='locationoverridecapacity' )

# result=valid2_rowcount(conn_src=conmsql ,p_source_schema='dbo',p_source_table='locationoverridecapacity',conn_tgt=connred ,p_target_schema='gasquest2019',p_target_table='balanceactivity' )
#
# print(result)
#

# result=valid_col_prepare_sql(p_dbtype='REDSHIFT_POSTGRES',p_table_name='locationoverridecapacity',p_column_name='comment',p_data_type='varchar')
# print(result)

#valid3_table_for_loop_cols(conn_src=conmsql,p_source_schema='dbo',p_source_table='locationoverridecapacity',conn_tgt=connred,p_target_schema='gasquest2019',p_target_table='locationoverridecapacity')


#mssql_create_export_views_json(p_conn_mssql,p_table_name,p_target_schema='SlalomMapping'):


# sql1=mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='locationoverridecapacity',p_target_schema='SlalomMapping')
# print(sql1)

#emergency1()

# lsql=red_list_all_tables(conmsql, 'dbo')
# print(len(lsql))
# lred=red_list_all_tables(connred, 'gasquest2019')
# print(len(lred))

valid_main_schema_compare(conn_src=conmsql,p_source_schema='dbo',conn_tgt=connred,p_target_schema='gasquest2019')
