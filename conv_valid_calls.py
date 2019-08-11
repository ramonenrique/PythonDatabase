import conv_validation as dbc
import sys




def compare_conversion_location_list():
	#Pending Need to set a way to write the connection properties dynamically, else just hardcode
	#based on the decision (easier)
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'LocationName', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'LocationZoneCode', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'LocationType', 'char', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'LocationClassification', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'NormalFlowIndicator', 'char', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'DateInService', 'datetime', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'DateFlowStart', 'datetime', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'City', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'CountyParish', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'StateCode', 'char', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'Section', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'Township', 'varchar', dbc.conn_aurora,
							   'location.Location');
	#dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'Range', 'varchar', dbc.conn_aurora,   'location.Location');

	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'PipelineCode', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'BlockValve', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'ValveMileage', 'decimal', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'Milepost', 'decimal', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'Latitude', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'Longitude', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'InertStationNumber', 'varchar', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'TSPID', 'int', dbc.conn_aurora, 'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'DateTimeAdded', 'datetime', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'DateTimeModified', 'datetime', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'LocationZoneID', 'int', dbc.conn_aurora,
							   'location.Location');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'PairLocationNumber', 'int',
							   dbc.conn_aurora, 'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'LocationAssociationTypeID', 'int',
							   dbc.conn_aurora, 'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'LocationPairName', 'varchar',
							   dbc.conn_aurora, 'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'DateEffective', 'datetime',
							   dbc.conn_aurora, 'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'DateExpire', 'int', dbc.conn_aurora,
							   'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'DateTimeAdded', 'datetime',
							   dbc.conn_aurora, 'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAssociation', 'DateTimeModified', 'datetime',
							   dbc.conn_aurora, 'location.LocationAssociation');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'AttributeID', 'int', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'AttributeFlag', 'int', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'DateEffective', 'datetime', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'DateTimeAdded', 'datetime', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationAttributes', 'DateTimeModified', 'datetime',
							   dbc.conn_aurora, 'location.LocationAttributes');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'LocationEntityRelationID', 'int',
							   dbc.conn_aurora, 'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'EntityID', 'int', dbc.conn_aurora,
							   'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'DateEffective', 'datetime', dbc.conn_aurora,
							   'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'DateTimeAdded', 'datetime', dbc.conn_aurora,
							   'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationEntityInfo', 'DateTimeModified', 'datetime',
							   dbc.conn_aurora, 'location.LocationEntityInfo');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'LocationNumber', 'int', dbc.conn_aurora,
							   'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'MAOP', 'int', dbc.conn_aurora,
							   'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'MAOPDescription', 'varchar',
							   dbc.conn_aurora, 'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'DateEffective', 'datetime',
							   dbc.conn_aurora, 'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'DateTimeAdded', 'datetime',
							   dbc.conn_aurora, 'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMaxPressure', 'DateTimeModified', 'datetime',
							   dbc.conn_aurora, 'location.LocationMaxPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'LocationNumber', 'int', dbc.conn_aurora,
							   'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'MinimumPSIG', 'int', dbc.conn_aurora,
							   'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'DateEffective', 'datetime',
							   dbc.conn_aurora, 'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'DateTimeAdded', 'datetime',
							   dbc.conn_aurora, 'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationMinPressure', 'DateTimeModified', 'datetime',
							   dbc.conn_aurora, 'location.LocationMinPressure');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'LocationNumber', 'int', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'OwnerEntityID', 'int', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'PercentOwnership', 'decimal', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'DateEffective', 'datetime', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'DateTimeAdded', 'datetime', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationOwner', 'DateTimeModified', 'datetime', dbc.conn_aurora,
							   'location.LocationOwner');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'LocationPropertyMasterID', 'int',
							   dbc.conn_aurora, 'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'LocationPropertyValue', 'varchar',
							   dbc.conn_aurora, 'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'DateEffective', 'datetime', dbc.conn_aurora,
							   'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'DateTimeAdded', 'datetime', dbc.conn_aurora,
							   'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'DateTimeModified', 'datetime',
							   dbc.conn_aurora, 'location.LocationProperty');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'LocationNumber', 'int', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'StatusCode', 'char', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'DateEffective', 'datetime', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'DateExpire', 'datetime', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'AddedBy', 'int', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'DateTimeAdded', 'datetime', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'ModifiedBy', 'int', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationStatus', 'DateTimeModified', 'datetime', dbc.conn_aurora,
							   'location.LocationStatus');
	dbc.save_xls(dbc.dfoutput,'conversion_output')


#dbc.compare_col_poly_in_db	(dbc.conn_mssql,	'gms.v_Location',	'Latitude'	,	'varchar'	,	dbc.conn_aurora	,	'location.Location'	);
#dbc.compare_col_poly_in_db	(dbc.conn_mssql,	'gms.v_Location',	'LocationName'	,	'varchar'	,	dbc.conn_aurora	,	'location.Location'	);
#dbc.save_xls(dbc.dfoutput,'conversion_output')

# dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'City', 'varchar', dbc.conn_aurora,  'location.Location');
# dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_Location', 'ValveMileage', 'decimal', dbc.conn_aurora,'location.Location');
# dbc.compare_col_poly_in_db	(dbc.conn_mssql,	'gms.v_Location',	'DateInService'	,	'datetime'	,	dbc.conn_aurora	,	'location.Location'	);
# dbc.compare_col_poly_in_db(dbc.conn_mssql, 'gms.v_LocationProperty', 'LocationPropertyMasterID', 'int', dbc.conn_aurora, 'location.LocationProperty');

compare_conversion_location_list()

dbc.save_xls(dbc.dfoutput, 'conversion_output')
