from enterprise.credentials import *
from enterprise.excel import *
from enterprise.db_generic import *

from enterprise.aurora_functions import *
from enterprise.mssql_functions import *
from enterprise.redshift_functions import *
from enterprise.validations import *

conmsql = connect_to_mssql()
# mssql_create_all_export_views_flat_file(conmsql)  # This will create ALL the tables. how about if they already exist?

#createst first time in new schema
#mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='DefaultFuelElection',p_view_schema='SlalomDataLake')

#x=mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='DefaultFuelElection',p_target_schema='SlalomMapping')

#overwrite current definition (should nto error out)
# mssql_create_export_views_json(p_conn_mssql=conmsql,p_table_name='_DefaultFuelElection',p_schema='SlalomDataLake'

# --               _     _     _  __ _
# --              | |   | |   (_)/ _| |
# --  _ __ ___  __| |___| |__  _| |_| |_
# -- | '__/ _ \/ _` / __| '_ \| |  _| __|
# -- | | |  __/ (_| \__ \ | | | | | | |_
# -- |_|  \___|\__,_|___/_| |_|_|_|  \__|

red_conn=red_plug_me_in()
# list_pop=red_list_pop_tables(red_conn,'gasquest2019')
# list_pop=red_list_pop_tables(red_conn,'gmsdw')

list_empty1=red_list_empty_tables(red_conn,'gasquest2019')
list_empty2=red_list_empty_tables(red_conn,'gmsdw')


