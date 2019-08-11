import migration_sql_to_rs as mig
import psycopg2
import pandas as pd

def compare_integration_list():
	# A batch refers to the a whole group of table validations that is kicked off at the same time.

	compare_integration('entity.Contract', 'gmsdwstg.entity_contract')
	compare_integration('entity.Country', 'gmsdwstg.entity_country')
	compare_integration('entity.Entity', 'gmsdwstg.entity_entity')
	compare_integration('entity.EntityAddress', 'gmsdwstg.entity_entityaddress')
	compare_integration('entity.StateProvince', 'gmsdwstg.entity_stateprovince')
	compare_integration('location.Line', 'gmsdwstg.location_line')
	compare_integration('location.LineCapacity', 'gmsdwstg.location_linecapacity')
	compare_integration('location.LineOverrideCapacity', 'gmsdwstg.location_lineoverridecapacity')
	compare_integration('location.LineSequence', 'gmsdwstg.location_linesequence')
	compare_integration('location.Location', 'gmsdwstg.location_location')
	compare_integration('location.LocationAttributes', 'gmsdwstg.location_locationattributes')
	compare_integration('location.LocationAttributesMaster', 'gmsdwstg.location_locationattributesmaster')
	compare_integration('location.LocationEntityInfo', 'gmsdwstg.location_locationentityinfo')
	compare_integration('location.LocationEntityInfoMaster', 'gmsdwstg.location_locationentityinfomaster')
	compare_integration('location.LocationGasQualityTemp', 'gmsdwstg.location_locationgasqualitytemp')
	compare_integration('location.LocationMaxPressure', 'gmsdwstg.location_locationmaxpressure')
	compare_integration('location.LocationMinPressure', 'gmsdwstg.location_locationminpressure')
	compare_integration('location.LocationOwner', 'gmsdwstg.location_locationowner')
	compare_integration('location.LocationProperty', 'gmsdwstg.location_locationproperty')
	compare_integration('location.LocationPropertyMaster', 'gmsdwstg.location_locationpropertymaster')
	compare_integration('location.LocationStatus', 'gmsdwstg.location_locationstatus')
	compare_integration('location.PMBalancingLocation', 'gmsdwstg.location_pmbalancinglocation')
	compare_integration('location.PMLine', 'gmsdwstg.location_pmline')
	compare_integration('location.PMLineSequence', 'gmsdwstg.location_pmlinesequence')
	compare_integration('location.PMLineSequenceConnectingLine', 'gmsdwstg.location_pmlinesequenceconnectingline')
	compare_integration('location.PMSequenceBoundZones', 'gmsdwstg.location_pmsequenceboundzones')
	compare_integration('location.PMSequenceLocation', 'gmsdwstg.location_pmsequencelocation')
	compare_integration('location.PMStatus', 'gmsdwstg.location_pmstatus')
	compare_integration('location.PMVersion', 'gmsdwstg.location_pmversion')
	compare_integration('location.Segment', 'gmsdwstg.location_segment')
	compare_integration('location.SegmentLocation', 'gmsdwstg.location_segmentlocation')
	compare_integration('nomination.Confirmation', 'gmsdwstg.nomination_confirmation')
	compare_integration('nomination.ConfirmationDetail', 'gmsdwstg.nomination_confirmationdetail')
	compare_integration('nomination.Cycle', 'gmsdwstg.nomination_cycle')
	compare_integration('nomination.mike_table', 'gmsdwstg.nomination_mike_table')
	compare_integration('nomination.MOCK_FuelRate', 'gmsdwstg.nomination_mock_fuelrate')
	compare_integration('nomination.Nomination', 'gmsdwstg.nomination_nomination')
	compare_integration('nomination.NominationDetail', 'gmsdwstg.nomination_nominationdetail')
	compare_integration('nomination.NominationScheduledQuantity', 'gmsdwstg.nomination_nominationscheduledquantity')
	compare_integration('nomination.NomScheduledQtyReductionReason',
						'gmsdwstg.nomination_nomscheduledqtyreductionreason')
	compare_integration('nomination.recon_commerce', 'gmsdwstg.nomination_recon_commerce')
	compare_integration('nomination.ReductionReason', 'gmsdwstg.nomination_reductionreason')
	compare_integration('nomination.SEOContractLocation', 'gmsdwstg.nomination_seocontractlocation')
	compare_integration('nomination.Status', 'gmsdwstg.nomination_status')
	compare_integration('nomination.zNomination', 'gmsdwstg.nomination_znomination')
	compare_integration('nomination.zNominationDetail', 'gmsdwstg.nomination_znominationdetail')
	compare_integration('nomination.z_MOCK_FuleRate', 'gmsdwstg.nomination_z_mock_fulerate')
	compare_integration('rfs.PrimaryPoints', 'gmsdwstg.rfs_primarypoints')
	compare_integration('rfs.QuantityReferenceType', 'gmsdwstg.rfs_quantityreferencetype')
	compare_integration('rfs.QuantityType', 'gmsdwstg.rfs_quantitytype')
	compare_integration('rfs.RequestPath', 'gmsdwstg.rfs_requestpath')
	compare_integration('rfs.ServiceRequest', 'gmsdwstg.rfs_servicerequest')

def process_migration_tables(max_iteration=5):

	v_sqlstmt=" select TABLE_NAME as table_name \
				from information_schema.TABLES t \
				where t.TABLE_SCHEMA = 'dbo' \
				and TABLE_TYPE = 'BASE TABLE' \
				order 	by 	table_name "

	try:
		df_result = pd.read_sql(v_sqlstmt, mig.conn_mssql)
		list_tables=df_result.values.tolist()
		limit_len = len(list_tables)  ##the result will only have one row ALWAYS, so getting the first row only.
		print('Getting ready to process lots of tables:',limit_len)

		for t in list_tables:
			current_table=t[0]
			v_source_table=current_table
			v_target_table='gasquest2019.' +current_table
			v_target_table=v_target_table.lower()

			print('Processing table:',v_source_table,'migrated to ',v_target_table)

			v_result=mig.valid1_table_structure(conn_src=mig.conn_mssql, p_source_table=v_source_table, conn_tgt=mig.conn_redshift, p_target_table=v_target_table,p_default_rows=500)
			if v_result == 'passed':
				v_result=mig.valid2_rowcount(conn_src=mig.conn_mssql, p_source_table=v_source_table,   conn_tgt=mig.conn_redshift, p_target_table=v_target_table)
			else:
				print('dummy: not callling rowcount because data structure failed')

			if v_result=='passed':
				mig.valid3_table_all_cols(conn_src=mig.conn_mssql, p_source_table=v_source_table,conn_tgt=mig.conn_redshift, p_target_table=v_target_table)
			else:
				print('dummy: not callling column validation because rowcount failed')
	except:
		print('process_migration_tables:***except reached****')

	#print(list_src)



def test_stuff():
	# try:
	# 	conn_redshift = psycopg2.connect("dbname=" + mig.redshift_database + " user=" + mig.redshift_user + " password=" + mig.redshift_password + " port=" + mig.redshift_port + " host=" + mig.redshift_host )
	# 	print('connected ok')
	# except:
	# 	print('still erroring out')

	# mig.valid1_table_structure(mig.conn_mssql,'NominationDetail',mig.conn_redshift,'gasquest2019.nominationdetail',p_default_rows=100)
	# mig.valid1_table_structure(mig.conn_mssql,'Allocation',mig.conn_redshift,'gasquest2019.allocation_rsalazar_fail1_struct',p_defaul
	# mig.valid2_rowcount(mig.conn_mssql,'Allocation',mig.conn_redshift,'gasquest2019.allocation_rsalazar_fail2_rowcount')t_rows=100)
	# mig.valid1_table_structure(mig.conn_mssql,'Allocation',mig.conn_redshift,'gasquest2019.allocation_rsalazar_fail2_rowcount',p_default_rows=100)
	# mig.valid1_table_structure(mig.conn_mssql,'NominationDetail',mig.conn_redshift,'gasquest2019.nominationdetail',p_default_rows=100)

	# mig.valid2_rowcount(mig.conn_mssql,'Allocation',mig.conn_redshift,'gasquest2019.allocation_rsalazar_fail1_struct')
	# print('REDSHIFT CONNECTOR 8:"',mig.conn_redshift.__str__()[1:8])
	# print('REDSHIFT CONNECTOR COMPLETE:"',mig.conn_redshift.__str__())
	# TEMPALTE FOR CALLING FUNCTIONG IS val_col_compare(conn_src,p_source_table,p_column_name,p_data_type,conn_tgt,p_target_table):

	# sql1=mig.valid_col_prepare_sql('MICROSOFT_SS','GasQuest.dbo.AllocationDetail','allocationtransactiontypecode','varchar')
	# print(sql1)
	#
	# sql2=mig.valid_col_prepare_sql('REDSHIFT_POSTGRES','gasquest2019.allocationdetail','allocationtransactiontypecode','varchar')
	# print(sql2)
	#
	# # def val_col_compare(conn_src,p_source_table,p_column_name,p_data_type,conn_tgt,p_target_table)
	#works as of 4:19
	#mig.valid3_table_all_cols(mig.conn_mssql, 'allocationdetail',mig.conn_redshift, 'gasquest2019.allocationdetail')
	#mig.val_col_compare(mig.conn_mssql,'GasQuest.dbo.AllocationDetail','allocationtransactiontypecode','varchar',mig.conn_redshift,'gasquest2019.allocationdetail')
	v_write_excel = 1

	#v_result = mig.valid2_rowcount(conn_src=mig.conn_mssql, p_source_table='AccountingPeriod', conn_tgt=mig.conn_redshift, p_target_table='gasquest2019.accountingperiod')

	#gasquest2019.bidratedetail

	process_migration_tables(3)

	if v_write_excel:
		print('saving log to xls')
		mig.save_xls(mig.dfoutput, "valid_migration")

test_stuff()


