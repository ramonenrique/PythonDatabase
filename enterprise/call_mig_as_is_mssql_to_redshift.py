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


conn_mssql=connect_to_mssql()
conn_redshift=connect_to_red()

#Three steps
#Step1:export data using bcp
#Step2:compress
#Step3:MANUAL (LOAD DATA INTO S3 IN BUCKET WITH SCHEMA NAME)
#Step4:RUN REDSHIFT copy command

#set database in master file credentials

#CONTROL OF VARIABLES
#





v_Filter_Case=""
v_Filter_Case="RFS_Notes"

try:
	v_filter_lowecase=v_Filter_Case.lower()
except:
	v_filter_lowecase = None

v_schema="qptm_gs"
step=1

print('FILTER:',v_filter_lowecase)

#Step1:export data using bcp
if step==1:
    mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql,p_source_schema='dbo',p_target_schema=v_schema,p_which_tables="filter:"+v_Filter_Case,p_mode="execute")

#Step2:compress
#path="C://temp//as_is_migration_large_tables//"
#Go to dos solution and run  the file (it is ready to go)

#Step3:MANUAL (LOAD DATA INTO S3 IN BUCKET WITH SCHEMA NAME)


#Step4:RUN REDSHIFT copy command
if step==4:
    red_run_copy_command_schema(conn_redshift=conn_redshift, p_mode='print', p_schema=v_schema,p_filter=v_filter_lowecase)


if step==5: #VALIDATION

	valid_main_schema_rowcount_only(conn_src=conn_mssql,p_source_schema='dbo',conn_tgt=conn_redshift,p_target_schema=v_schema,p_filter=v_Filter_Case)

