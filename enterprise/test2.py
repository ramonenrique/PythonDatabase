from enterprise.credentials import *
from enterprise.excel  import *
from enterprise.db_generic  import *

from enterprise.aurora_functions  import *
from enterprise.mssql_functions  import *
from enterprise.redshift_functions  import *
from enterprise.validations import *


def call_bus_val_test1():
	global conn_redshift
	global v_glob_cur
	global conn_mssql

	valid_bus_cus_views(conn_src=conn_mssql, p_source_view='slalom.v_custom_valid1', conn_tgt=conn_redshift, p_target_view='validation.v_custom_valid1', p_column_order_by='locationstatusdescription')


#MAIN

# call_bus_val_test1()
# print_bcp_all()  #create_export_views()

#print('all is ok')
# cr=red_plug_me_in()
# cur=cr.cursor

#red_run_list_copy_command(p_list='empty', p_mode='print', target_schema='gasquest2019')
#temp_print_emtpy()
#le=red_list_empty_tables('gasquest2019')
#print(le)
#print_bcp_all

conn_mssql=connect_to_mssql()

p_test_check_exists("Location_Affidahhvit.json")