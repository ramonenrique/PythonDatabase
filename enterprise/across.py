from enterprise.credentials import *
from enterprise.excel import *
from enterprise.db_generic import *

from enterprise.aurora_functions import *
from enterprise.mssql_functions import *
from enterprise.redshift_functions import *
from enterprise.validations import *


#FUNCTION:get the list of reamining tables to migrate.
#TIP some tables in the source are empty, so those need to be skipped(there is nothing to load)
#If we do not remove these tables from the list, they always show as pending

#STEPS ARE
#(1) get list of pending tables from redshift environment (still zero)
#(2) get list of zero rowcount tables from source environment
#substract l1-l2


def mig_list_pending_migrate(connmsql,connred,p_source_schema,p_target_schema):
#The source schema is always dbo, but the target shcema chan change

    list_empty=red_list_empty_tables(connred,p_schema=p_target_schema)
    list_zero=mssql_list_empty_tables(connmsql,p_target_schema='dbo')

    list_zero_lower=[]
    for item in list_zero: list_zero_lower.append(item.lower())

    if type(list_empty) == list and type(list_zero)== list:
        print('just checking')
        list_pending=(list(set(list_empty) - set(list_zero_lower)))
        print('original length:',len(list_empty),'new length',len(list_pending))
    else:
        print('one of my variables is not a list')

    return(list_pending)





