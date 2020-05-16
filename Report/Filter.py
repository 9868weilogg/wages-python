from lib import DateTime;
from lib import Logger;
from lib import fn;
from lib import Params;
from lib import SharedMemoryManager;
from Report import File;
import json;
import copy;

main_folder = 'crawled_data';
structure_map_name = {
	'state': {
		'seq_no': 'state_seq_no',
		'name': 'state_name',
		'code': 'state_code',
	},
	'ptj': {
		'seq_no': 'ptj_seq_no',
		'name': 'ptj_name',
		'code': 'ptj_code',
	},
	'facility': {
		'seq_no': 'facility_seq_no',
		'name': 'facility_name',
		'code': 'facility_code',
	},
	'facility_type': {
		'seq_no': 'facility_type',
		'name': 'facility_type',
		'code': 'facility_type', #XXX
	},
}
def getDropdownList(params):
	result = {};
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	custom_params = copy.deepcopy(params);

	for key in ['state', 'ptj', 'facility', 'facility_type']:
		custom_params['key'] = key;
		# read from file
		# filename = '{0}/{1}/2020-02-28.json'.format(main_folder, key);
		# data = File.readLatestFile(directory='/'.join([main_folder, key]), extension='.json');

		# read from mongodb
		try:
			data = list(db[key].find({}, {'_id': 0}));
		except Exception as ex:
			Logger.v(ex);
			data = File.readLatestFile(directory='/'.join([main_folder, key]), extension='.json');

		# Logger.v('data', data);
		accessible_data = getAccessibleData(params=custom_params, data=data);

		result[key] = organiseStructure(data=accessible_data, key=key);

	result['duration'] = [
		{
			'id': 'yearly',
			'name': 'Yearly',
		},
		{
			'id': 'monthly',
			'name': 'Monthly',
		},
	];
	result['year'] = [
		{
			'id': 2020,
			'name': '2020',
		},
		{
			'id': 2019,
			'name': '2019',
		},
	];
	result['procurement_type'] = [
		{
			'id': 'type1',
			'name': 'Type 1',
		},
		{
			'id': 'type2',
			'name': 'Type 2',
		},
	];
	result['budget_type'] = [
		{
			'id': 'db',
			'name': 'Dasar Baru',
		},
		{
			'id': 'oo',
			'name': 'One Off',
		},
	];
	return Params.generate(True, result);

def getAccessibleData(params, data):
	Logger.v('getAccessibleData,', 'params:', params, 'data:', len(data));

	return data;

def organiseStructure(data, key):
	limit = 10;
	result = [];
	if data:
		if key in ['state', 'ptj', 'facility', 'facility_type']:
			# generate id for 'all' option #XXX
			# all_value = [];
			# for idx in range(0, len(data)):
			# 	row = data[idx];
			# 	code = fn.getNestedElement(structure_map_name, '{0}.code'.format(key), '');
			# 	val = fn.getNestedElement(row, code);
			# 	# Logger.v('code', code, 'val', val, 'key', key)
			# 	if val not in all_value:
			# 		all_value.append(val);

			# obj_ = {
			# 	'id': ','.join(all_value),	
			# 	'name': 'all',	
			# }
			# if obj_ not in result:
			# 	result.append(obj_);

			# generate id for each option
			for idx in range(0, len(data)):
				row = data[idx];
				seq_no = fn.getNestedElement(structure_map_name, '{0}.seq_no'.format(key), '');
				name = fn.getNestedElement(structure_map_name, '{0}.name'.format(key), '');
				code = fn.getNestedElement(structure_map_name, '{0}.code'.format(key), '');
				obj_ = {
					# use code as 'id':
					'id': fn.getNestedElement(row, code),	
					'name': fn.getNestedElement(row, name),	

					# use seq_no as 'id'
					# 'id': fn.getNestedElement(row, seq_no),	
					# 'name': fn.getNestedElement(row, name),	
					# 'code': fn.getNestedElement(row, code),	
				}
				if obj_ not in result:
					result.append(obj_);

	return result[:limit];