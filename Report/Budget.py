import copy;

from lib import Logger;
from lib import fn;
from lib import DebugManager;
from lib import Params;
from lib import SharedMemoryManager;
from Report import Filter;
from Report import File;
from Report import Report;

global_group_order = {
	'procurement': ['facility_type', 'facility', 'drug'],	
	'budget': ['state', 'facility', 'budget_type', 'object', 'item_group'],
};
global_group_order_kepmap = {
	'facility_type': 'facility_type',
	'drug': 'drug_code',
	'facility': 'facility_code',
	'budget_type': 'budget_type_code',
	'object': 'object_code',
	'item_group': 'item_group_code',
	'state': 'state_code',
};
global_report_key_update = {
	'budget': [
		'first_allocation', 'additional_allocation', 'pending_amount', 'utilized_amount', 'liablity_amount', 'trans_in_amount', 'trans_out_amount', 'deduction_amount', 'current_actual_amount', 'total_allocation', 'balance_amount'
	],
	'procurement': [
		'e_p_approved_quantity', 'purchase_amount',
	],
};
global_children_key = {
	'budget': [
		'first_allocation', 'additional_allocation', 'pending_amount', 'utilized_amount', 'liablity_amount', 'total_allocation', 'balance_amount', 'trans_in_amount', 'trans_out_amount', 'deduction_amount', 'current_actual_amount'
	],
	'procurement': [
		'e_p_approved_quantity', 'min_unit_price', 'max_unit_price', 'purchase_amount', 'item_packaging_seq_no', 'item_packaging_name',
	],
};

def get(params):
	# Debug = DebugManager.DebugManager();
	# Debug.start();
	# Debug.trace('start')

	report_name = Report.getReportName(params);
	Logger.v('-----Start Getting Data-----');
	data = getData(params=params);
	# Debug.trace('get data')
	Logger.v('-----Start Calculate Data-----');
	calculated_data = calculateData(params=params, data=data);
	# Debug.trace('calculate data')
	Logger.v('-----Start Restructure Data-----');
	result = toOutputStructure(params=params, data={'data': data, 'calculated_data': calculated_data});
	# Debug.trace('structure data')
	# Debug.end();
	# Debug.show('Budget.get');
	return result;


def getData(params):
	report_name = Report.getReportName(params);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	limit_data = None; # 5000 (used 30+- second)
	query = generateQuery(params=params);
	# Logger.v('query', query);

	data = list(db[report_name].find(query, {'_id':0}))[:limit_data]; # direct load from mongodb

	# data = File.readJson('crawled_data/testing_purpose/{0}.json'.format(report_name)); # TEST using demo data

	# data = File.readJson('crawled_data/testing_purpose/pivot_{0}.json'.format(report_name)); # TEST using pivot data
	# lambda_function = lambda d: d['ptj_code'] in ['010619'];
	# data = list(filter(lambda_function, data));
	# fn.writeExcelFile(filename='crawled_data/testing_purpose/pivot_{0}'.format(report_name), data=data);
	# fn.writeJSONFile(filename='crawled_data/testing_purpose/pivot_{0}.json'.format(report_name), data=data);

	Logger.v('data length', len(data));
	return data;

def generateQuery(params):
	report_name = Report.getReportName(params)
	filter_params = {};
	query = {};
	filter_keymap = {
		'procurement': {
			'year': 'txn_year',
			'months': 'txn_month',
			'facility_type': 'facility_type',
			'facility': 'facility_code',
			'ptj': 'ptj_code',
			'state': 'state_code',
			'procurement_type': 'procurement_type',
		},
		'budget': {
			'year': 'financial_year',
			'facility_type': 'facility_type',
			'facility': 'facility_code',
			'ptj': 'ptj_code',
			'state': 'state_code',
			'budget_type': 'budget_type_code',
		},
	}
	filters = fn.getNestedElement(params, 'filter', {});
	for f in list(filters.keys()):
		key = fn.getNestedElement(filter_keymap, '{0}.{1}'.format(report_name, f), None);
		if key:
			if key not in filter_params:
				filter_params[key] = [];
			values = fn.getNestedElement(filters, f, '').split(',');
			for val in values:
				if val:
					if key == 'txn_month':
						filter_params[key].append(val.zfill(2));

					filter_params[key].append(val);

	for fp in filter_params:
		val = filter_params[fp];
		if val:
			query[fp] = { '$in': val };

	return query;

def calculateData(params, data):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');
	custom_params = copy.deepcopy(params);
	report_name = Report.getReportName(params);
	group_order = global_group_order[report_name];
	result = {};
	unique_keys = generateUniqueKeys(params, data);
	Debug.trace('get unique_keys');

	for uk in unique_keys:
		# Logger.v('uk', uk);
		split_uk = uk.split('_');
		# Logger.v('split_uk', split_uk);
		# Logger.v('lenght split_uk', len(split_uk));
		custom_params['split_uk'] = split_uk;

		functions = generateFilterFunction(params=custom_params);
		function_name = 'function_{0}'.format(len(split_uk));

		# Logger.v('functions', functions);
		filtered_data = list(filter(functions[function_name], data));
		# Logger.v('filtered_data', filtered_data);

		# Logger.v('sum of first allocation', sum(list(obj_['first_allocation'] for obj_ in filtered_data)));
		total_keys = global_report_key_update[report_name];
		# Debug.trace('get filtered_data');

		if filtered_data:
			item = generateItemDetail(params=custom_params, data=filtered_data);

			if uk not in result:
				result[uk] = {};

			result[uk] = {
				'id': item['id'],
				'name': item['name'],
				'code': item['code'],
				'total': {},
			}
			for tk in total_keys:
				try:
					result[uk]['total'][tk] = sum(list(obj_[tk] for obj_ in filtered_data));
				except Exception as KeyError:
					# Logger.v('Report.calculateData: {0} not found, sum up after data cleaning process.'.format(tk));
					if tk == 'total_allocation':
						result[uk]['total'][tk] = result[uk]['total']['first_allocation'] + result[uk]['total']['additional_allocation'];
					elif tk == 'balance_amount':
						result[uk]['total'][tk] = result[uk]['total']['first_allocation'] + result[uk]['total']['additional_allocation'] - result[uk]['total']['pending_amount'] - result[uk]['total']['liablity_amount'] - result[uk]['total']['utilized_amount'];

				# Logger.v('tk', tk);
		# Debug.trace('calculating data');

	# Logger.v('result', result);
	Debug.end();
	Debug.show('Report.calculateData');
	return result;

def generateUniqueKeys(params, data):
	report_name = Report.getReportName(params);
	structure_keymap = fn.getNestedElement(params, 'structure_keymap');
	unique_keys = [];

	for idx in range(0, len(data)):
		row = data[idx];
		key = {};
		key['facility_type'] = fn.getNestedElement(row, 'facility_type');
		key['facility_code'] = fn.getNestedElement(row, 'facility_code');
		key['budget_type_code'] = fn.getNestedElement(row, 'budget_type_code');
		key['object_code'] = fn.getNestedElement(row, 'object_code');
		key['drug_code'] = fn.getNestedElement(row, 'drug_code');
		key['item_group_code'] = fn.getNestedElement(row, 'item_group_code');
		key['state_code'] = fn.getNestedElement(row, 'state_code');

		push_data = [];
		for sm in global_group_order[report_name]:
			mapped_key = global_group_order_kepmap[sm];
			push_data.append(key[mapped_key]);
			unique_keys.append('_'.join(push_data));

	# Logger.v('unique_keys', len(unique_keys));
	unique_keys = sorted(list(set(unique_keys)));
	# Logger.v('unique_keys', len(unique_keys));
	return unique_keys;

def generateFilterFunction(params):
	split_uk = fn.getNestedElement(params, 'split_uk');
	report_name = Report.getReportName(params);
	result = {}
	function_name = 'function_{0}'.format(len(split_uk));
	if len(split_uk) == 1:
		if report_name == 'budget':
			result[function_name] = lambda d: d['state_code'] == split_uk[0];
		elif report_name == 'procurement':
			result[function_name] = lambda d: d['facility_type'] == split_uk[0];
	elif len(split_uk) == 2:
		if report_name == 'budget':
			result[function_name] = lambda d: d['state_code'] == split_uk[0] and d['facility_code'] == split_uk[1];
		elif report_name == 'procurement':
			result[function_name] = lambda d: d['facility_type'] == split_uk[0] and d['facility_code'] == split_uk[1];
	elif len(split_uk) == 3:
		if report_name == 'budget':
			result[function_name] = lambda d: d['state_code'] == split_uk[0] and d['facility_code'] == split_uk[1] and d['budget_type_code'] == split_uk[2];
		elif report_name == 'procurement':
			result[function_name] = lambda d: d['facility_type'] == split_uk[0] and d['facility_code'] == split_uk[1] and d['drug_code'] == split_uk[2];
	elif len(split_uk) == 4:
		if report_name == 'budget':
			result[function_name] = lambda d: d['state_code'] == split_uk[0] and d['facility_code'] == split_uk[1] and d['budget_type_code'] == split_uk[2] and d['object_code'] == split_uk[3];
	elif len(split_uk) == 5:
		if report_name == 'budget':
			result[function_name] = lambda d: d['state_code'] == split_uk[0] and d['facility_code'] == split_uk[1] and d['budget_type_code'] == split_uk[2] and d['object_code'] == split_uk[3] and d['item_group_code'] == split_uk[4];
	return result;

def generateItemDetail(params, data):
	result = {};
	report_name = Report.getReportName(params);
	split_uk = fn.getNestedElement(params, 'split_uk');

	for sm in global_group_order[report_name][:len(split_uk)]:
		if sm == 'facility_type':
			smk = 'facility';
		else:
			smk = sm;
		if sm == 'facility_type':
			id_ = '{0}_type'.format(smk);
			name_ = '{0}_type'.format(smk);
			name_2 = '{0}_type'.format(smk);
			code_ = '{0}_type'.format(smk);
		else:
			id_ = '{0}_seq_no'.format(smk);
			name_ = '{0}_name'.format(smk);
			name_2 = '{0}_desc'.format(smk);
			code_ = '{0}_code'.format(smk);

	try:
		result['name'] = data[0][name_];
	except Exception as e:
		result['name'] = fn.getNestedElement(data[0], name_2, fn.getNestedElement(data[0], code_))

	result['id'] = data[0][id_];
	result['code'] = data[0][code_];

	return result;

def toOutputStructure(params, data):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');
	new_data = fn.getNestedElement(data, 'data');
	calculated_data = fn.getNestedElement(data, 'calculated_data');
	report_name = Report.getReportName(params);
	min_purchase_amount = fn.getNestedElement(params, 'filter.min_purchase_amount', 0);
	if type(min_purchase_amount) == str:
		min_purchase_amount = int(min_purchase_amount);
	total_keys = global_report_key_update[report_name];
	result = {};
	all_keys = global_group_order[report_name];
	keys = {};
	for idx in range(0, len(all_keys)):
		ak = all_keys[idx];
		i = str(idx);
		keys['key'+i] = ak;


	new_data_0 = copy.deepcopy(new_data);
	portion_0 = [];
	unique_values0 = sorted(list(set(obj_[global_group_order_kepmap[keys['key0']]] for obj_ in new_data_0)));

	for uv0 in unique_values0:
		new_data_1 = list(filter(lambda d: d[global_group_order_kepmap[keys['key0']]] == uv0, new_data_0))
		portion_1 = [];
		unique_values1 = sorted(list(set(obj_[global_group_order_kepmap[keys['key1']]] for obj_ in new_data_1)));
		join_key_1 = '_'.join([uv0]);

		for uv1 in unique_values1:
			new_data_2 = list(filter(lambda d: d[global_group_order_kepmap[keys['key1']]] == uv1, new_data_1))
			portion_2 = [];
			unique_values2= sorted(list(set(obj_[global_group_order_kepmap[keys['key2']]] for obj_ in new_data_2)));
			join_key_2 = '_'.join([uv0, uv1]);

			if report_name == 'budget':
				for uv2 in unique_values2:
					new_data_3 = list(filter(lambda d: d[global_group_order_kepmap[keys['key2']]] == uv2, new_data_2));
					portion_3 = [];
					unique_values3= sorted(list(set(obj_[global_group_order_kepmap[keys['key3']]] for obj_ in new_data_3)));
					join_key_3 = '_'.join([uv0, uv1, uv2]);

					for uv3 in unique_values3:
						new_data_4 = list(filter(lambda d: d[global_group_order_kepmap[keys['key3']]] == uv3, new_data_3));
						portion_4 = [];
						unique_values4= sorted(list(set(obj_[global_group_order_kepmap[keys['key4']]] for obj_ in new_data_4)));
						join_key_4 = '_'.join([uv0, uv1, uv2, uv3]);

						for uv4 in unique_values4: # last key
							new_data_5 = list(filter(lambda d: d[global_group_order_kepmap[keys['key4']]] == uv4, new_data_4));
							join_key_5 = '_'.join([uv0, uv1, uv2, uv3, uv4]);
							children = generateChildren(params=params, data=new_data_5);
							if children:
								portion_4.append({
									'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_5)),
									'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_5)),
									'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_5)),
									'total': generateSummary(params=params, data=children),
									'children': children,	
								});
						if portion_4:
							portion_3.append({
								'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_4)),
								'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_4)),
								'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_4)),
								'total': generateSummary(params=params, data=portion_4),
								keys['key4']: portion_4,	
							});
					if portion_3:
						portion_2.append({
							'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_3)),
							'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_3)),
							'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_3)),
							'total': generateSummary(params=params, data=portion_3),
							keys['key3']: portion_3,	
						});
				if portion_2:
					portion_1.append({
						'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_2)),
						'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_2)),
						'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_2)),
						'total': generateSummary(params=params, data=portion_2),
						keys['key2']: portion_2,	
					});
			elif report_name == 'procurement':
				for uv2 in unique_values2:
					new_data_3 = list(filter(lambda d: d[global_group_order_kepmap[keys['key2']]] == uv2, new_data_2));
					join_key_3 = '_'.join([uv0, uv1, uv2]);
					children = generateChildren(params=params, data=new_data_3);
					if children:
						portion_2.append({
							'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_3)),
							'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_3)),
							'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_3)),
							'total': generateSummary(params=params, data=children),
							'children': children,	
						});
				if portion_2:
					portion_1.append({
						'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_2)),
						'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_2)),
						'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_2)),
						'total': generateSummary(params=params, data=portion_2),
						keys['key2']: portion_2,	
					});
		if portion_1:
			portion_0.append({
				'id': fn.getNestedElement(calculated_data, '{0}.id'.format(join_key_1)),
				'name': fn.getNestedElement(calculated_data, '{0}.name'.format(join_key_1)),
				'code': fn.getNestedElement(calculated_data, '{0}.code'.format(join_key_1)),
				'total': generateSummary(params=params, data=portion_1),
				keys['key1']: portion_1,	
			});
	if portion_0:
		result[report_name] = {
			'group_order': global_group_order[report_name],
			'total': generateSummary(params=params, data=portion_0),
			keys['key0']: portion_0,	
		};
	Debug.end();
	Debug.show('toOutputStructure');
	return result;

def generateChildren(params, data):
	report_name = Report.getReportName(params);
	min_purchase_amount = fn.getNestedElement(params, 'filter.min_purchase_amount', 0);
	if type(min_purchase_amount) == str and min_purchase_amount:
		min_purchase_amount = int(min_purchase_amount);
	children = [];
	for d in data:
		# Logger.v('d', d);
		obj_ = {};
		# Logger.v('global_children_key',global_children_key)
		# Logger.v('report_name',report_name)
		for gck in global_children_key[report_name]:
			# Logger.v('gck', gck)
			value = fn.getNestedElement(d, gck);
			if value or value == 0:
				obj_[gck] = value;
			else:
				if report_name == 'budget':
					# Logger.v('Report.generateChildren: {0} not found, sum up after data cleaning process.'.format(gck));
					if gck == 'total_allocation':
						obj_[gck] = d['first_allocation'] + d['additional_allocation'];
					elif gck == 'balance_amount':
						obj_[gck] = d['first_allocation'] + d['additional_allocation'] - d['pending_amount'] - d['liablity_amount'] - d['utilized_amount'];

		# Logger.v('obj_', obj_);
		if report_name == 'procurement':
			if obj_['purchase_amount'] > min_purchase_amount:
				children.append(obj_);
		else:
			children.append(obj_);
	return children;

def generateSummary(params, data):
	report_name = Report.getReportName(params);
	total_keys = global_report_key_update[report_name];
	result = {};
	for tk in total_keys:
		try:
			result[tk] = sum(list(obj_['total'][tk] for obj_ in data));
		except Exception as ex:
			# Logger.v('generateSummary', ex);
			result[tk] = sum(list(obj_[tk] for obj_ in data));

	return result;