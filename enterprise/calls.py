from credentials import *
from excel  import *
from db_generic  import *

from aurora_functions  import *
from mssql_functions  import *
from redshift_functions  import *
from validations import *


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
conn_redshift=connect_to_red()
print("DATABASE:",mssql_db)

conn_aurora=connect_me_to_aurora()

#mssql_create_export_views_flat(conn_mssql)


#mssql_create_export_views_json(connect_to_mssql(),p_table_name='ContractQuantity') #works_mon_night

#mssql_export_bcp(p_table='balanceactivity',p_src_schema='slalommapping',p_prefix='v_export_',p_mode='print',p_file_type='json')

#mssql_export_bcp(p_table='balanceactivity',p_src_schema='slalommapping',p_prefix='v_export_',p_mode='execute',p_file_type='json')

# mssql_create_export_views_loop_tables(p_conn_mssql=conn_mssql)

#mssql_export_bcp(p_table='balanceactivity',p_src_schema='slalommapping',p_prefix='v_export_',p_mode='print',p_file_type='json')

#qj=mssql_create_query_json(p_conn_mssql=conn_mssql,p_schema="dbo",p_table_name="Rates_CalcIndices")
# print(qj)
#
# #run_bcp(p_test=2)

# x=db_list_all_tables(p_conn=conn_mssql,p_schema="dbo")
# print(x)
#
# v_jsonqry=mssql_create_query_json(p_conn_mssql=conn_mssql,p_schema="dbo",p_table_name="ba_Credit")
# print(v_jsonqry)
#
#mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="Billing_InvoiceDetail", p_mode='print', p_file_type='json')

#mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql,p_schema="dbo",p_which_tables="pending",p_mode="print")

#
# x=red_list_empty_tables(conn_redshift, 'qptm_gc')
# print(x)
#
# # #
#red_run_copy_command_for_loop(conn_redshift=conn_redshift, p_mode='execute', p_schema='qptm_gc')

#
# #red_run_copy_command(conn_redshift=conn_redshift,p_schema='qptm_gs', p_table="ba_contact_roles_btm",p_mode='execute')
#
# # a=db_get_row_count_in_db(conn1=conn_redshift,p_schema='gasquest2019', p_table="allocation")
# # print(a)
# # b=db_get_row_count_in_db(conn1=conn_redshift,p_schema='qptm_gs', p_table="ba_contact_roles_btm")
# # print(b)
#
# l1=red_list_empty_tables(p_connred=conn_redshift, p_schema='qptm_gs')
# print(l1)

# x=mssql_create_query_json(p_conn_mssql=conn_mssql,p_schema="dbo",p_table_name="Billing_InvGrpContracts")
# print(x)

# x=mssql_list_filter_tables(conn_mssql,"dbo","Contract_")
# print(x)

# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_Credit"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_Credit_Line"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_Default_Agents"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_Entity_Addresses"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_EntityCredit"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_OtherIDs"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="Billing_InvGrpContracts"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="Billing_InvGrpCopies"	, p_mode='execute', p_file_type='json')
# # mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="Billing_InvGrpDocuments"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="Contract_Agents"	, p_mode='execute', p_file_type='json')

# #red_run_copy_command(conn_redshift=conn_redshift,p_schema='qptm_gs', p_table="ba_contact_roles_btm",p_mode='execute')

#20190906 PENDING TO MAKE IT EASY TO CHANGE THE DATABSE/SCHEMA WITH PARAMETERS
#step1 mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql,p_schema="dbo",p_which_tables="all",p_mode="execute")
#
# # #step3
# x=red_list_all_tables(p_conn_red=conn_redshift, p_schema="qptm_gc")
# # print(x)
# # print(x["table_name"])
#
# # # y=red_list_pop_tables(p_conn_red=conn_redshift, p_schema="gptm_gc")
# # # print(y)
# #
# z=red_list_empty_tables(p_cr=conn_redshift,p_schema='qptm_gc')
# print(z)
# # #
# red_run_copy_command_schema(conn_redshift=conn_redshift, p_mode='execute', p_schema="qptm_gc")
# [dbo].[BA_Classification] pending with 291
# [Billing_InvHdrMaint] 11444
#
#
# billing_invhdrmaint.json.gz
# ba_classification.json.gz
#
#
# red_run_copy_command(conn_redshift,'qptm_gc', 'Billing_InvHdrMaint',p_mode='execute')
# red_run_copy_command(conn_redshift,'qptm_gc', 'BA_Classification',p_mode='execute')
#
#
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="Billing_InvHdrMaint"	, p_mode='execute', p_file_type='json')
# mssql_export_bcp_json(conn_mssql,p_src_schema="dbo", p_table="BA_Classification"	, p_mode='execute', p_file_type='json')


#valid_main_schema_rowcount_only(conn_src=conn_mssql,p_source_schema='dbo',conn_tgt=conn_redshift,p_target_schema='qptm_gs',p_filter=None)

#valid_main_schema_rowcount_only(conn_src=conn_mssql,p_source_schema='dbo',conn_tgt=conn_redshift,p_target_schema='qptm_gc',p_filter="Rates_")

#mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql, p_source_schema='dbo', p_target_schema="qptm_gs",	 p_which_tables="pending", p_mode="print")


#def mssql_list_pending_migrate_case_proof(p_conn_mssql,p_source_schema,p_conn_red,p_target_schema):
# pl=mssql_list_pending_migrate_case_proof(p_conn_mssql=conn_mssql,p_source_schema='dbo',p_conn_red=conn_redshift,p_target_schema='qptm_gs')
# print(pl)


lsch=db_list_all_schemas(conn_aurora)
print(lsch)


