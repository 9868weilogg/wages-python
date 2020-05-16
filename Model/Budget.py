import pandas as pd;
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
global_report_key_update = {
	'budget': [
		'first_allocation', 'additional_allocation', 'pending_amount', 'utilized_amount', 'liablity_amount', 'trans_in_amount', 'trans_out_amount', 'deduction_amount', 'current_actual_amount', 'total_allocation', 'balance_amount'
	],
	'procurement': [
		'e_p_approved_quantity', 'purchase_amount',
	],
};
global_key_to_join = {
	'procurement': ['facility_type', 'facility_code', 'drug_code', 'min_unit_price', 'max_unit_price', 'item_packaging_seq_no'],	
	'budget': ['state_name', 'facility_code', 'budget_type_code', 'object_code', 'item_group_code'],
};
global_process_order = {
	'procurement': ['facility_type', 'facility_code', 'drug_code', '_'.join(global_key_to_join['procurement'])],	
	'budget': ['state_name', 'facility_code', 'budget_type_code', 'object_code', 'item_group_code', '_'.join(global_key_to_join['budget'])],
};

def get(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start')

	report_name = Report.getReportName(params);
	Logger.v('-----Start Getting Data-----');
	data = getData(params=params);
	Debug.trace('get data')
	Logger.v('-----Start Calculate Data-----');
	calculated_data = calculateData(params=params, data=data);
	Debug.trace('calculate data')
	Logger.v('-----Start Restructure Data-----');
	result = toOutputStructure(params=params, data={'data': data, 'calculated_data': calculated_data});
	Debug.trace('structure data')
	Debug.end();
	Debug.show('Model.Procurement.get');
	return result;


def getData(params):
	report_name = Report.getReportName(params);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	limit_data = 5000; # 5000 (used 30+- second)
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
	processed_df = preprocessData(params, data);
	# Logger.v('processed_df', processed_df);
	# processed_df.info();
	grouped_df = groupData(params=params, data=processed_df);
	report_name = Report.getReportName(params);
	key_to_join = fn.getNestedElement(global_key_to_join, report_name);
	result = {};
	joined_key = [];
	joined_ = [];
	joined_columns_list = [key_to_join[0]];
	for idx in range(0, len(key_to_join)):
		ktj = key_to_join[idx];
		joined_key.append(ktj);

		if idx > 0:		
			joined_.append(['_'.join(joined_key[:-1]), ktj]);
			columns = joined_[idx-1];
			joined_columns = '_'.join(columns);
			joined_columns_list.append(joined_columns);
	last_key = joined_columns_list[-1];
	# Logger.v('last_key', last_key);

	for index, row in grouped_df.iterrows():
		# Logger.v('row', row);
		unique_value = row[last_key];
		reference = {
			'value': {},
			'po': {},
			'name': {
				0: row['state_code'],
				1: row['facility_name'],
				2: row['budget_type_name'],
				3: row['object_name'],
				4: row['item_group_name'],
				5: row['item_group_name'],
			}
		};
		for idx in range(0, len(global_process_order[report_name])):
			po = global_process_order[report_name][idx];
			# Logger.v('po', po);
			# Logger.v('po value', row[po]);
			reference['value'][idx] = row[global_process_order[report_name][idx]];
			reference['po'][idx] = global_process_order[report_name][idx];

			value_ref = reference['value'];
			po_ref = reference['po'];
			name_ref = reference['name'];
			
			if idx > 0:
				keys = [];
				for idx1 in range(0, idx):
					keys += [po_ref[idx1], value_ref[idx1]];
					# Logger.v('keys', keys);
				key = '.'.join(keys);
				# Logger.v('idx', idx, 'key', key);
				check_temp_result = fn.getNestedElement(result, key);
			else:
				check_temp_result = result;

			if po_ref[idx] not in check_temp_result:
				check_temp_result[po_ref[idx]] = {};

			if po_ref[idx] == 'state_name':
				name = value_ref[idx];
				code = name_ref[idx];
			else:
				name = name_ref[idx];
				code = value_ref[idx];

			if value_ref[idx] not in check_temp_result[po_ref[idx]]:
				if idx == len(global_process_order[report_name]) - 1:
					# Logger.v('row', row);
					# exit();
					obj_ = {
						'id': fn.convertToSnakecase(code),
						'name': name,
						'code': code,
						'first_allocation': float(row['first_allocation']),
						'additional_allocation': float(row['additional_allocation']),
						'pending_amount': float(row['pending_amount']),
						'utilized_amount': float(row['utilized_amount']),
						'liablity_amount': float(row['liablity_amount']),
						'trans_in_amount': float(row['trans_in_amount']),
						'trans_out_amount': float(row['trans_out_amount']),
						'deduction_amount': float(row['deduction_amount']),
						'current_actual_amount': float(row['current_actual_amount']),
						'total_allocation': float(row['total_allocation']),
						'balance_amount': float(row['balance_amount']),
					};
				else:
					obj_ = {
						'id': fn.convertToSnakecase(code),
						'name': name,
						'code': code,
					};
				check_temp_result[po_ref[idx]][value_ref[idx]] = obj_

		# exit();
	# Logger.v('result', result);
	# filename = 'william_py_result';
	# fn.writeTestFile(filename, result, minified=False);
	# exit();
	return result;

def preprocessData(params, data):
	report_name = Report.getReportName(params);
	key_to_join = fn.getNestedElement(global_key_to_join, report_name);
	df = pd.DataFrame(data, dtype=str);
	# Logger.v('df', df);
	joined_key = [];
	joined_ = [];
	joined_columns_list = [key_to_join[0]];
	df['first_allocation'] = pd.to_numeric(df['first_allocation']);
	df['additional_allocation'] = pd.to_numeric(df['additional_allocation']);
	df['pending_amount'] = pd.to_numeric(df['pending_amount']);
	df['utilized_amount'] = pd.to_numeric(df['utilized_amount']);
	df['liablity_amount'] = pd.to_numeric(df['liablity_amount']);
	df['trans_in_amount'] = pd.to_numeric(df['trans_in_amount']);
	df['trans_out_amount'] = pd.to_numeric(df['trans_out_amount']);
	df['deduction_amount'] = pd.to_numeric(df['deduction_amount']);
	df['current_actual_amount'] = pd.to_numeric(df['current_actual_amount']);
	df['total_allocation'] = pd.to_numeric(df['total_allocation']);
	df['balance_amount'] = pd.to_numeric(df['balance_amount']);
	# df.info();
	for idx in range(0, len(key_to_join)):
		ktj = key_to_join[idx];
		joined_key.append(ktj);

		if idx > 0:		
			joined_.append(['_'.join(joined_key[:-1]), ktj]);
			columns = joined_[idx-1];
			joined_columns = '_'.join(columns);
			joined_columns_list.append(joined_columns);
			df[joined_columns] = df[columns[0]].str.cat(df[columns[1]], sep ="|");
	return df;

def groupData(params, data):
	report_name = Report.getReportName(params);
	key_to_join = fn.getNestedElement(global_key_to_join, report_name);
	joined_key = [];
	joined_ = [];
	joined_columns_list = [key_to_join[0]];
	for idx in range(0, len(key_to_join)):
		ktj = key_to_join[idx];
		joined_key.append(ktj);

		if idx > 0:		
			joined_.append(['_'.join(joined_key[:-1]), ktj]);
			columns = joined_[idx-1];
			joined_columns = '_'.join(columns);
			joined_columns_list.append(joined_columns);
	last_key = joined_columns_list[-1];
	# Logger.v('last_key', last_key);
	grouped_df = data.groupby([last_key])[['first_allocation', 'additional_allocation', 'pending_amount', 'utilized_amount', 'liablity_amount', 'trans_in_amount', 'trans_out_amount', 'deduction_amount', 'current_actual_amount', 'total_allocation', 'balance_amount']].apply(sum).reset_index();
	# Logger.v('grouped_df', grouped_df[[last_key, 'purchase_amount']]);

	for idx in range(0, len(key_to_join)):
		key = key_to_join[idx];
		grouped_df[key] = grouped_df[last_key].str.split(pat='|', expand=True)[idx];

	for naming_key in ['facility_name', 'state_code', 'item_group_name', 'budget_type_name', 'object_name']:
		ec_df = data[[last_key, naming_key]].drop_duplicates();
		mapping_dict = dict(zip(ec_df[last_key].tolist(), ec_df[naming_key].tolist()));
		grouped_df[naming_key] = grouped_df[last_key].map(mapping_dict);
	return grouped_df;

def toOutputStructure(params, data):
	report_name = Report.getReportName(params);
	calculated_data = fn.getNestedElement(data, 'calculated_data');
	result = {};
	process_orders = global_process_order[report_name];
	group_orders = global_group_order[report_name];

	key0_data = calculated_data[process_orders[0]];
	# Logger.v('key0_data', key0_data);
	idx_count = 1;
	portion0 = [];

	for key0 in key0_data:
		# Logger.v('key0', key0);
		key1_data = key0_data[key0][process_orders[1]];
		# Logger.v('idx_count', idx_count, len(process_orders) - 1);
		if idx_count < len(process_orders) - 1:
			idx_count += 1;
			portion1 = [];

			for key1 in key1_data:
				key2_data = key1_data[key1][process_orders[2]];
				# Logger.v('key2_data', key2_data);
				# Logger.v('idx_count', idx_count, len(process_orders) - 1);
				if idx_count < len(process_orders) - 1:
					idx_count += 1;
					portion2 = [];
					for key2 in key2_data:
						# Logger.v('idx_count', idx_count, len(process_orders) - 1);

						key3_data = key2_data[key2][process_orders[3]];
						# Logger.v('key3_data', key3_data);
						if idx_count < len(process_orders) - 1:
							idx_count += 1;
							pass;
							portion3 = [];
							for key3 in key3_data:
								# Logger.v('idx_count', idx_count, len(process_orders) - 1);

								key4_data = key3_data[key3][process_orders[4]];
								if idx_count < len(process_orders) - 1:
									idx_count += 1;
									portion4 = [];
									for key4 in key4_data:
										# Logger.v('idx_count', idx_count, len(process_orders) - 1);

										key5_data = key4_data[key4][process_orders[5]];
										if idx_count < len(process_orders) - 1:
											idx_count += 1;
											pass;
											idx_count -= 1;
										else:
											children = [];
											for child_key in key5_data:
												row = key5_data[child_key];
												obj_ = {
													'first_allocation': row['first_allocation'],
													'additional_allocation': row['additional_allocation'],
													'pending_amount': row['pending_amount'],
													'utilized_amount': row['utilized_amount'],
													'liablity_amount': row['liablity_amount'],
													'trans_in_amount': row['trans_in_amount'],
													'trans_out_amount': row['trans_out_amount'],
													'deduction_amount': row['deduction_amount'],
													'current_actual_amount': row['current_actual_amount'],
													'total_allocation': row['total_allocation'],
													'balance_amount': row['balance_amount'],
												};
												children.append(obj_);
											if children:
												portion4.append({
													'id': key4_data[key4]['id'],
													'name': key4_data[key4]['name'],
													'code': key4_data[key4]['code'],
													'total': generateSummary(params, children),
													'children': children,
												});
												# Logger.v('child total', generateSummary(params, children));
												# exit();
									
									idx_count -= 1;
								else:
									pass;
								if portion4:
									portion3.append({
										'id': key3_data[key3]['id'],
										'name': key3_data[key3]['name'],
										'code': key3_data[key3]['code'],
										'total': generateSummary(params, portion4),
										group_orders[4]: portion4,
									});
								
							idx_count -= 1;
						else:
							pass;
						if portion3:
							portion2.append({
								'id': key2_data[key2]['id'],
								'name': key2_data[key2]['name'],
								'code': key2_data[key2]['code'],
								'total': generateSummary(params, portion3),
								group_orders[3]: portion3,
							});
					idx_count -= 1;
					
				else:
					pass;
				if portion2:
					portion1.append({
						'id': key1_data[key1]['id'],
						'name': key1_data[key1]['name'],
						'code': key1_data[key1]['code'],
						'total': generateSummary(params, portion2),
						group_orders[2]: portion2,
					});
					# Logger.v('portion2 total', generateSummary(params, portion2));
			idx_count -= 1;

		else:
			pass;
		if portion1:
			portion0.append({
				'id': key0_data[key0]['id'],
				'name': key0_data[key0]['name'],
				'code': key0_data[key0]['code'],
				'total': generateSummary(params, portion1),
				group_orders[1]: portion1,
			})
			# Logger.v('portion0', portion0);

	result[report_name] = {
		'group_order': group_orders,
		'total': generateSummary(params, portion0),
		group_orders[0]: portion0,
	};

	# filename = 'william_py_result';
	# fn.writeTestFile(filename, result, minified=False);
	# exit();
	return result;

def generateSummary(params, data):
	report_name = Report.getReportName(params);
	total_keys = global_report_key_update[report_name];
	result = {};
	for tk in total_keys:
		try:
			result[tk] = sum(list(obj_['total'][tk] for obj_ in data));
		except Exception as ex:
			# Logger.v('generateSummary', ex);
			try:
				result[tk] = sum(list(obj_[tk] for obj_ in data));
			except Exception as ex:
				result[k] = 0;
	return result;