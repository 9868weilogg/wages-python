from lib import DateTime;
from lib import File;
from lib import fn;
from lib import SharedMemoryManager;
from lib import DebugManager;
from lib import Logger;
from lib import Params;

from Model import StockIssue as ModelStockIssue;
from Model import Stock as ModelStock;

import pandas as pd;
import numpy as np;
import copy;

project_root_folder = fn.getRootPath();
reference_folder = '{0}/reference/'.format(project_root_folder);
key_with_code_name = ['ptj', 'facility', 'requester', 'drug_nondrug', 'item'];
key_to_join = {
	'state': ['state', 'facility_code', 'requester_group_code', 'drug_nondrug_code', 'packaging', 'item_code', 'sku'],
	'facility': ['state', 'facility_code', 'requester_group_code', 'drug_nondrug_code', 'packaging', 'item_code', 'sku'],
};
process_order = {
	'state': ['facility_code', 'requester_group_code', 'drug_nondrug_code', '_'.join(key_to_join['state'])],
	'facility': ['requester_group_code', 'drug_nondrug_code', '_'.join(key_to_join['facility'])],
};
item_key_to_show = {
	'state': ['facility_code', 'requester_group_code', 'facility_name', 'requester_group_name', 'drug_nondrug_code', 'drug_nondrug_name', 'packaging', 'sku', 'issue_type', 'item_group_name'],
	'facility': ['facility_code', 'facility_name', 'requester_group_code', 'requester_group_name', 'drug_nondrug_code', 'drug_nondrug_name', 'packaging', 'sku', 'issue_type', 'item_group_name'],
};
group_by = 'state';

def getOption(params):
	data = [];
	filename = '{0}/stock_issue_options.json'.format(reference_folder);
	data = File.readJSONFile(filename);
	return data;

def check(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');
	global process_order;
	global key_to_join;
	global group_by;
	group_by_list = fn.getNestedElement(params, 'group_by', []);
	filter_quantity = fn.getNestedElement(params, 'filter.quantity', []);
	export = fn.getNestedElement(params, 'export', None);
	custom_params = copy.deepcopy(params);
	result = {};

	# filter and read mongo db 
	data = ModelStockIssue.get(params);
	Debug.trace('read mongo');

	# filtering by group 
	if group_by_list:
		group_by = group_by_list[-1]['id'];
	else:
		group_by = 'state';

	custom_params['group_by_key'] = group_by;
	custom_params['process_order'] = process_order[group_by];
	custom_params['key_to_join'] = key_to_join[group_by];
	custom_params['item_key_to_show'] = item_key_to_show[group_by];

	# processing data 
	if data:
		temp_result = ModelStockIssue.calculateData(params=custom_params, data=data);
		Debug.trace('calculate data');
		result = toOutputStructure(params=custom_params, data=temp_result);
		Debug.trace('structure data');

	if result == {}:
		result = [];
	Debug.end();
	Debug.show('StockIssue.run');
	if export:
		export_result = generateExcelStructure(params=custom_params, data=result);
		return Params.generate(True, export_result);
	else:
		return Params.generate(True, result);

def toOutputStructure(params, data):
	process_order = fn.getNestedElement(params, 'process_order');
	filter_quantity = fn.getNestedElement(params, 'filter.quantity', []);
	total_po_length = len(process_order);
	result = [];
	idx_count = 0;
	for level_0 in data:
		level_0_data = data[level_0];
		if idx_count < total_po_length - 1:
			idx_count += 1;
			poidx1 = process_order[idx_count]; 
			portion_1 = [];
			for level_1 in fn.getNestedElement(level_0_data, poidx1, []):
				level_1_data = level_0_data[poidx1][level_1];
				if idx_count < total_po_length - 1:
					idx_count += 1;
					poidx2 = process_order[idx_count]; 
					portion_2 = [];
					for level_2 in level_1_data[poidx2]:
						level_2_data = level_1_data[poidx2][level_2];
						if idx_count < total_po_length - 1:
							idx_count += 1;
							poidx3 = process_order[idx_count]; 
							portion_3 = [];
							for level_3 in level_2_data[poidx3]:
								level_3_data = level_2_data[poidx3][level_3];

								filterQuantity(params=params, data={'main': portion_3, 'sub': level_3_data});
							addData(params={'key': poidx3, 'process_order': process_order}, data={'main': portion_2, 'sub': portion_3, 'misc': level_2_data});
							idx_count -= 1;
						else:
							filterQuantity(params=params, data={'main': portion_2, 'sub': level_2_data});
					addData(params={'key': poidx2, 'process_order': process_order}, data={'main': portion_1, 'sub': portion_2, 'misc': level_1_data});
					idx_count -= 1;
				else:
					filterQuantity(params=params, data={'main': portion_1, 'sub': level_1_data});
			addData(params={'key': poidx1, 'process_order': process_order}, data={'main': result, 'sub': portion_1, 'misc': level_0_data});
			idx_count -= 1;
	result = stockIssueSummary(params=params, data=result);
	return result;

def stockIssueSummary(params, data):
	process_order = fn.getNestedElement(params, 'process_order');
	result = copy.deepcopy(data);
	# Logger.v('result', result);
	idx_count = 0;
	total_po_length = len(process_order);
	for idx in range(0, len(result)):
		if idx_count < total_po_length - 1:
			idx_count += 1;
			row = result[idx];
			# Logger.v('row', row);
			key = renameDictKey(key=process_order[idx_count], process_order=process_order);
			level_1_data = row[key];
			summary_data = {};
			for idx1 in range(0, len(level_1_data)):
				if idx_count < total_po_length - 1:
					idx_count += 1;
					row1 = level_1_data[idx1];
					# Logger.v('row1', row1);
					key = renameDictKey(key=process_order[idx_count], process_order=process_order);
					level_2_data = row1[key];
					for idx2 in range(0, len(level_2_data)):
						row2 = level_2_data[idx2];
						# Logger.v('row2', row2);
						if idx_count < total_po_length - 1:
							idx_count += 1;
							key = renameDictKey(key=process_order[idx_count], process_order=process_order);
							level_3_data = row2[key];
							for idx3 in range(0, len(level_3_data)):
								row3 = level_3_data[idx3];

								summary_data = insertLastElement(reference=row3, data=summary_data);
							idx_count -= 1;

						else:
							summary_data = insertLastElement(reference=row2, data=summary_data);
					idx_count -= 1;
			# Logger.v('summary_data', summary_data);
			row['summary'] = summaryByRow(params=params, data=summary_data);
			idx_count -= 1;
	# Logger.v('result', result)
	return result;

def insertLastElement(reference, data):
	result = copy.deepcopy(data);
	quantity = reference['quantity'];
	drug_nondrug_code = reference['drug_nondrug_code'];
	if drug_nondrug_code not in result:
		result[drug_nondrug_code] = {};

	unique_id = '_'.join([reference['code'], reference['sku']]);

	if unique_id not in result[drug_nondrug_code]:
		result[drug_nondrug_code][unique_id] = copy.deepcopy(reference);
		# Logger.v('result 1', unique_id, result[drug_nondrug_code][unique_id])
	else:
		result[drug_nondrug_code][unique_id]['quantity'] += quantity;	
		# Logger.v('result 2', unique_id, result[drug_nondrug_code][unique_id])
		for month in reference['quantity_by_month']:
			result[drug_nondrug_code][unique_id]['quantity_by_month'][month] += reference['quantity_by_month'][month];
	return result;

def summaryByRow(params, data):
	result = {
		'overall': [],
		'detail': {},
	};
	month_range = ModelStockIssue.getMonthRange(params);

	if data:
		for cd in data:
			result['detail'][cd] = [data[cd][item] for item in data[cd]];
			result['detail'][cd] = sorted(result['detail'][cd], key=lambda k: k['quantity'], reverse=True);

		for drug_code in result['detail']:
			obj_ = {
				'id': drug_code,
				'quantity': 0,
			}
			if 'quantity_by_month' not in obj_:
				obj_['quantity_by_month'] = {};
			for month in month_range:
				obj_['quantity_by_month'][month] = 0;

			for item in result['detail'][drug_code]:
				# Logger.v('item', item)
				obj_.update({
					'name': item['drug_nondrug_name'],
					'code': item['drug_nondrug_code'],
				});
				obj_['quantity'] += item['quantity'];
				for month in item['quantity_by_month']:
					obj_['quantity_by_month'][month] += item['quantity_by_month'][month]; 

			result['overall'].append(obj_);
		result['overall'] = sorted(result['overall'], key=lambda k: k['quantity'], reverse=True);
	return result;

def addData(params, data):
	key = fn.getNestedElement(params, 'key');
	process_order = fn.getNestedElement(params, 'process_order');
	main = fn.getNestedElement(data, 'main');
	sub = fn.getNestedElement(data, 'sub');
	misc = fn.getNestedElement(data, 'misc');
	if sub:
		portion_key = renameDictKey(key=key, process_order=process_order);
		main.append({
			'id': misc['id'],
			'name': misc['name'],
			'code': misc['code'],
			portion_key: sub,
		});

def filterQuantity(params, data):
	filter_quantity = fn.getNestedElement(params, 'filter.quantity', None);
	main = fn.getNestedElement(data, 'main');
	sub = fn.getNestedElement(data, 'sub');
	qty = sub['quantity'];
	can_append = ModelStock.quantityWithinRange(params=params, quantity=qty);
	if can_append or not filter_quantity:
		main.append(sub);

def renameDictKey(key, process_order):
	if key == process_order[-1]:
		return 'item';
	else:
		return key.replace('_code', '');

def generateExcelStructure(params, data):
	result = [];
	metadata = {};
	group_by_key = fn.getNestedElement(params, 'group_by_key');
	column_to_add = {
		'all': ['state_name', 'drug_nondrug_name', 'code', 'packaging', 'quantity_by_month', 'sku'],
		'state': ['facility_name', 'drug_nondrug_name', 'code', 'packaging', 'quantity_by_month', 'sku'],
		'facility': ['requester_group_name', 'drug_nondrug_name', 'code', 'packaging', 'quantity_by_month', 'sku'],
		'requester': ['drug_nondrug_name', 'code', 'packaging', 'batch_no', 'expiry_date', 'quantity_by_month', 'sku'],
	}
	name_mapping = {
		'state_name': 'state',
		'facility_name': 'facility',
		'requester_group_name': 'requester group',
		'code': 'item code',
		'drug_nondrug_name': 'drug/non-drug name',
		'sku': 'sku',
		'quantity': 'quantity',
		'batch_no': 'batch no',
		'expiry_date': 'expiry date',
		'packaging': 'packaging description',
	}
	new_data = [];
	# Logger.v('asd', data);
	for idx in range(0, len(data)):
		row = data[idx];
		for drug_code in row['summary']['detail']:
			new_data += row['summary']['detail'][drug_code];

	for idx in range(0, len(new_data)):
		row = new_data[idx];
		# Logger.v('row', row)
		state_name = row['id'].split('|')[0].replace('_', ' ');
		obj_ = {};
		for col in column_to_add[group_by_key]:
			if col == 'state_name':
				obj_.update({
					name_mapping[col]: state_name,
				});
			elif col =='quantity_by_month':
				for month in row[col]:
					date = '{0}-01'.format(month);
					month_string = DateTime.getDateCategoryName(date=date, element='month')[:3].lower();
					# Logger.v('month_string', month_string);
					key_name = 'quantity ({0})'.format(month_string);
					obj_.update({
						key_name: row[col][month],	
					});
			else:
				obj_.update({
					name_mapping[col]: row[col],
				});
		# Logger.v('group_by_key', group_by_key);
		result.append(obj_);
		metadata = generateExportMeta(params=params, data={'state_name': state_name, 'row': row});

	# Logger.v('result', result);
	# Logger.v('metadata', metadata);
	return {
		'data': result, 
		'metadata': metadata,
	};

def generateExportMeta(params, data):
	metadata = {};
	group_by_key = fn.getNestedElement(params, 'group_by_key');
	state_name = fn.getNestedElement(data, 'state_name');
	row = fn.getNestedElement(data, 'row');
	if group_by_key == 'all':
		metadata['group_by'] = 'combine';
	elif group_by_key == 'state':
		metadata['group_by'] = group_by_key;
		metadata['state_name'] = state_name;
	elif group_by_key == 'facility':
		metadata['group_by'] = group_by_key;
		metadata['state_name'] = state_name;
		metadata['facility_name'] = row['facility_name'];
	elif group_by_key == 'requester':
		metadata['group_by'] = group_by_key;
		metadata['state_name'] = state_name;
		metadata['facility_name'] = row['facility_name'];
		metadata['requester_unit_desc'] = row['requester_unit_desc'];
	return metadata;