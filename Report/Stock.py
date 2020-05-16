from lib import SharedMemoryManager;
from lib import DebugManager;
from lib import fn;
from lib import Logger;
from lib import DateTime;
from lib import File;

from Model import Stock as ModelStock;
from Model import StockIntegrity as ModelStockIntegrity;
from Model import Structure as ModelStructure;
from Model import Upload as ModelUpload;

from pymongo import TEXT;
import pandas as pd;
import pytz;
import json;
import copy;
import requests;
import asyncio;

from . import Item;
from Model import Item as ModelItem;

key_to_join = {
	'all': ['state', 'facility_code', 'requester_unit_code','packaging', 'item_code', 'sku'],
	'state': ['state', 'facility_code', 'requester_unit_code', 'packaging', 'item_code', 'sku'],
	'facility': ['state', 'facility_code', 'requester_unit_code', 'packaging', 'item_code', 'sku'],
	'requester': ['state', 'facility_code', 'requester_unit_code', 'packaging', 'item_code', 'sku', 'batch_no', 'expiry_date', 'unit_price', 'item_group', 'sub_group_code', 'item_category_code', 'pku_code', 'drug_nondrug_code', 'requester_group_code', 'ptj_code'],
	'drug': ['state', 'facility_code', 'requester_unit_code', 'packaging', 'item_code', 'sku' , 'batch_no', 'expiry_date', 'unit_price', 'item_group', 'sub_group_code', 'item_category_code', 'pku_code', 'drug_nondrug_code', 'requester_group_code', 'ptj_code'],
};
process_order = {
	'all': ['state', 'facility_code', '_'.join(key_to_join['all'])],
	'state': ['facility_code', 'requester_unit_code', '_'.join(key_to_join['state'])],
	'facility': ['requester_unit_code', '_'.join(key_to_join['facility'])],
	'requester': ['item_code', '_'.join(key_to_join['requester'])],
	'drug': ['item_code', '_'.join(key_to_join['drug'])],
};
item_key_to_show = {
	'all': ['facility_code', 'requester_unit_code', 'facility_name', 'requester_unit_desc', 'packaging', 'sku'],
	'state': ['facility_code', 'requester_unit_code', 'facility_name', 'requester_unit_desc', 'packaging', 'sku'],
	'facility': ['facility_code', 'facility_name', 'requester_unit_code', 'requester_unit_desc', 'packaging', 'sku'],
	'requester': ['facility_code', 'facility_name', 'requester_unit_code', 'requester_unit_desc', 'packaging', 'sku', 'batch_no', 'expiry_date', 'unit_price', 'item_group', 'sub_group_code', 'item_category_code', 'pku_code', 'drug_nondrug_code', 'requester_group_code', 'ptj_code', 'sub_group_desc', 'item_category_desc', 'pku_desc', 'drug_nondrug_desc', 'requester_group_name', 'ptj_name'],
	'drug': ['facility_code', 'facility_name', 'requester_unit_code', 'requester_unit_desc', 'packaging', 'sku', 'batch_no', 'expiry_date', 'unit_price', 'item_group', 'sub_group_code', 'item_category_code', 'pku_code', 'drug_nondrug_code', 'requester_group_code', 'ptj_code', 'sub_group_desc', 'item_category_desc', 'pku_desc', 'drug_nondrug_desc', 'requester_group_name', 'ptj_name'],
};
group_by = 'all';

def run(params):
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
	data = ModelStock.get(params);
	Debug.trace('read mongo');

	# filtering by group 
	if group_by_list:
		group_by = group_by_list[-1]['id'];
	else:
		group_by = 'all';

	custom_params['group_by_key'] = group_by;
	custom_params['process_order'] = process_order[group_by];
	custom_params['key_to_join'] = key_to_join[group_by];
	custom_params['item_key_to_show'] = item_key_to_show[group_by];

	# processing data 
	if data:
		temp_result = ModelStock.calculateData(params=custom_params, data=data);
		Debug.trace('calculate data');
		result = ModelStructure.stockCheck(params=custom_params, data=temp_result);
		Debug.trace('structure data');

	if result == {}:
		result = [];
	Debug.end();
	Debug.show('Stock.run');
	if export:
		export_result = generateExcelStructure(params=custom_params, data=result);
		return export_result;
	else:
		return result;

def triggerOnComplete(params):
	qid = fn.getNestedElement(params, 'id');
	api_url = fn.getNestedElement(params, 'callback_url', '');
	if not api_url:
		Logger.v('No Callback Url. Skip trigger.');
		return;
	# if not 'https://' in url:
	# 	url = 'https://{0}'.format(url);
	# api_url = '{0}/{1}'.format(url.strip('/'),qid); # staging api
	Logger.v('Calling:', api_url);
	x = requests.get(api_url, data={'result': json.dumps(params)}, verify=False);

	Logger.v('x', x.text);
	Logger.v('result', params);

def getBackdateList(params):
	result = ModelStock.getBackdateList(params);
	return result;

def generateExcelStructure(params, data):
	result = [];
	metadata = {};
	group_by_key = fn.getNestedElement(params, 'group_by_key');
	column_to_add = {
		'all': ['state_name', 'code', 'name', 'quantity', 'sku'],
		'state': ['facility_name', 'code', 'name', 'quantity', 'sku'],
		'facility': ['requester_unit_desc', 'code', 'name', 'quantity', 'sku'],
		'requester': ['code', 'name', 'batch_no', 'expiry_date', 'quantity', 'sku'],
	}
	name_mapping = {
		'state_name': 'state',
		'facility_name': 'facility',
		'requester_unit_desc': 'requester unit',
		'code': 'code',
		'code': 'code',
		'name': 'drug/non-drug name',
		'sku': 'sku',
		'quantity': 'quantity',
		'batch_no': 'batch no',
		'expiry_date': 'expiry date',
	}
	for row0 in data:
		for row1 in row0['summary']:
			state_name = row1['id'].split('|')[0].replace('_', ' ');
			obj_ = {};
			for col in column_to_add[group_by_key]:
				if col == 'state_name':
					obj_.update({
						name_mapping[col]: state_name,
					});
				else:
					obj_.update({
						name_mapping[col]: row1[col],
					});

			result.append(obj_);
			if group_by_key == 'all':
				metadata['group_by'] = 'combine';
			elif group_by_key == 'state':
				metadata['group_by'] = group_by_key;
				metadata['state_name'] = state_name;
			elif group_by_key == 'facility':
				metadata['group_by'] = group_by_key;
				metadata['state_name'] = state_name;
				metadata['facility_name'] = row1['facility_name'];
			elif group_by_key == 'requester':
				metadata['group_by'] = group_by_key;
				metadata['state_name'] = state_name;
				metadata['facility_name'] = row1['facility_name'];
				metadata['requester_unit_desc'] = row1['requester_unit_desc'];

	# Logger.v('result', result);
	# Logger.v('metadata', metadata);
	return {
		'data': result, 
		'metadata': metadata,
	};