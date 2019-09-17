# The data needs to be sorted out first, and the index needs to be removed
# Assuming the first column is a Primary key, else the whole table needs to be sorted out (all columns)
# need to identify the first column of the list
# The data needs to be sorted out by the same field
# The index needs to be reset, otherwise the comparison will not work.

def fdo_val_col_compare(conn_src,p_source_sch_table,p_column_name,p_data_type,conn_tgt,p_target_sch_table):


    dfs=conn_src.read_sql(select )
    v_first_column = list_columns_aurora[0]
    df_standarized_src = df_standarized_src.sort_values(by=[v_first_column]).reset_index(drop=True)
    df_standarized_tgt = df_standarized_tgt.sort_values(by=[v_first_column]).reset_index(drop=True)

    v_equal_data_set = df_standarized_src.equals(df_standarized_tgt)

    if v_equal_data_set:
        print('Validation x passed: All rows and columns are the same:', '@time:', time.ctime())
        audit_row_dict['val_result'] = "passed"
        audit_row_dict['val_check5_full_data'] = 'passed'
    else:
        print(
            'VALIDATION x failed: Data sets are different. You may want to run a discrepancy test with function dfDiff',
            '@time:', time.ctime())
        audit_row_dict['val_check5_full_data'] = 'failed'
        audit_row_dict['val_result'] = "failed"
        return -5

# IF this point is reached, then the structure is the same and the data is the same
print('FINAL RESULT passed: All validations passed sucessfully', '@time:', time.ctime())
val_result = 'passed'


	# AUTHOR: RAMON SALAZAR
	#    DATE: 2019-07-10
	# FUNCTION: Compares the data at a summary level. For numeric fields sum(x) and avg(x)
	# The table name in the target database can change. The column name MUST be the same.
	#The list that will be passed to compare_col_poly_in_db needs to be hardcoded
	#because the table names vary
	global dfoutput
	v_save_two_sqlstmt=''

	v_column_name=p_column_name.lower()
