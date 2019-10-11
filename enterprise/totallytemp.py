from enterprise.inv_credentials import *
from enterprise.excel  import *
from enterprise.db_generic  import *

from enterprise.aurora_functions  import *
from enterprise.mssql_functions  import *
from enterprise.redshift_functions  import *
from enterprise.validations import *
from enterprise.dos_util import *
import time


conn_mssql=connect_to_mssql()
conn_redshift=connect_to_red()

#Step1:export data using bcp
#Step2:compress
#Step3:MANUAL (LOAD DATA INTO S3 IN BUCKET WITH SCHEMA NAME)
#Step4:RUN REDSHIFT copy command
#Step5:Validation

v_schema="demo_qptm_gs"
v_table="Billing_InvoiceDetail" #larger table (2 min in server)
v_table="BA_TSP"
v_table="Location_Geographic"

v_table_lower_redshift=v_table.lower()

start_step=5
end_step=5

#Step1:export data using bcp
# t0= time.perf_counter()
# print("Time start:", t0)  # CPU seconds elapsed (floating point)

t1s= time.perf_counter()
if  start_step<=1<=end_step:
    #FULL_SCHEMA: mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql,p_source_schema='dbo',p_target_schema=v_schema,p_which_tables="filter:"+v_Filter_Case,p_mode="execute")
    mssql_export_bcp_json(p_conn_mssql=conn_mssql, p_src_schema="dbo", p_table=v_table, p_mode='print', p_file_type='json')
    t1e = time.perf_counter()
    print("Time elapsed Step 1 ", t1e - t1s) # CPU seconds elapsed (floating point)

t2s= time.perf_counter()
if  start_step<=2<=end_step:
    path="C://temp//as_is_migration_large_tables//"
    dos_compress_files(path)
    t2e = time.perf_counter()
    print("Time elapsed Step 2 Seconds: ", t2e - t2s) # CPU seconds elapsed (floating point)

t3s= time.perf_counter()
if  start_step<=3<=end_step:
    print("aws cli move to s3")
    t3e = time.perf_counter()
    print("Time elapsed Step 3 Seconds: ", t3e - t3s) # CPU seconds elapsed (floating point)

    #Step3:MANUAL (LOAD DATA INTO S3 IN BUCKET WITH SCHEMA NAME)

t4s= time.perf_counter()
if  start_step<=4<=end_step:
    #schema red_run_copy_command_schema(conn_redshift=conn_redshift, p_mode='execute', p_schema=v_schema,p_filter=v_filter_lowercase)
    #one table
    red_run_copy_command(conn_redshift, v_schema.lower(),v_table.lower(), p_mode="execute")
    t4e = time.perf_counter()
    print("Time elapsed Step 4 Seconds: ", t4e - t4s) # CPU seconds elapsed (floating point)

t5s= time.perf_counter()
if  start_step<=5<=end_step:
	#schema valid_main_schema_rowcount_only(conn_src=conn_mssql,p_source_schema='dbo',conn_tgt=conn_redshift,p_target_schema=v_schema,p_filter=v_Filter_Case)
    #termplate=def valid2_rowcount(conn_src, p_source_schema, p_source_table, conn_tgt, p_target_schema, p_target_table):
    valid2_rowcount(conn_src=conn_mssql, p_source_schema='dbo', p_source_table=v_table, conn_tgt=conn_redshift, p_target_schema=v_schema, p_target_table=v_table_lower_redshift)
    t5e = time.perf_counter()
    print("Time elapsed Step 5:Seconds: ", t5e - t5s)  # CPU seconds elapsed (floating point)


