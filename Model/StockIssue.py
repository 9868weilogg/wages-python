from lib import DebugManager;
from lib import SharedMemoryManager;
from lib import Logger;
from lib import fn;
from lib import File;
from lib import DateTime;

from Model import Stock as ModelStock;
from Model import Upload as ModelUpload;
from Model import StockIssueIntegrity as ModelSIIntegrity;

from Report import Stock as ReportStock;

import asyncio;
import copy;
from pymongo import TEXT;
import pandas as pd;

collection_name = 'stock_issue';
chunksize = int(fn.getNestedElement(fn.config, 'UPLOAD_CHUNK_SIZE','2500'));
stock_issue_options = {};
project_root_folder = fn.getRootPath();
reference_folder = '{0}/reference/'.format(project_root_folder);
column_keymap = {
	'STATE_NAME': 'state',
	'PTJ_CODE': 'ptj_code',
	'PTJ_NAME': 'ptj_name',
	'FACILITY_CODE': 'facility_code',
	'FACILITY_NAME': 'facility_name',
	'DEI_RQSTR_CODE': 'requester_code',
	'DEI_RQSTR_DESC': 'requester_name',
	'DEI_GROUP_NAME': 'requester_group_name',
	'DEI_ITEM_TYPE': 'item_type',
	'DEI_ITEM_GROUP': 'item_group',
	'DEI_CAT_DESC': 'item_category_name',
	'DEI_SUBGROUP_DESC': 'sub_group_name',
	'DEI_DRUG_NONDRUG_CODE': 'drug_nondrug_code',
	'DEI_DRUG_NONDRUG_DESC': 'drug_nondrug_name',
	'DEI_ITEM_CODE': 'item_code',
	'DEI_ITEM_DESC': 'item_name',
	'DEI_PACKAGING': 'packaging',
	'DEI_CONVERSION_FACTOR': 'conversion_factor',
	'DEI_PKU_UOM_DESC': 'pku_name',
	'DEI_COUNTRY_OF_ORIGIN': 'country_of_origin',
	'DEI_PRODUCT_TYPE': 'product_type',
	'DEI_BRD_NAME': 'brand_name',
	'DEI_MANU_SHORTNAME': 'manufacturer_shortname',
	'DEI_APPROVED_DATE': 'approved_date',
	'DEI_ISSUE_QTY': 'issue_quantity',
	'DEI_UOM_DESC': 'sku',
	'DEI_BATCH_NO': 'batch_no',
	'DEI_EXPIRY_DATE': 'expiry_date',
	'DEI_TRANSACTION_TYPE': 'issue_type',
	'DEI_ISSUED_TO': 'issue_to',
	'DEI_UNIT_PRICE': 'unit_price',
};

def get(params):
	drug_codes = fn.getNestedElement(params, 'drug_nondrug_code', []);
	state = fn.getNestedElement(params, 'state');
	requester_group = fn.getNestedElement(params, 'requester_group');
	issue_type = fn.getNestedElement(params, 'issue_type');
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	match_query = {
		'state': state.replace('_', ' '),
	};
	if drug_codes:
		match_query['drug_nondrug_code'] = {'$in': drug_codes};

	if not requester_group == 'all' and requester_group:
		match_query['requester_group_name'] = requester_group.replace('_', ' ');

	if not issue_type == 'all' and issue_type:
		match_query['issue_type'] = issue_type.replace('_', ' ');

	data = list(db[collection_name].aggregate([
		{
			'$match': match_query,
		},
		{
			'$project': {'_id': 0, 'inserted_at': 0, 'updated_at': 0}
		}
	]));
	data_length = len(data);
	# Logger.v('data length', data_length, data);
	return data;

def calculateData(params, data):
	global naming_keymap, crawl_folder;
	item_key_to_show = fn.getNestedElement(params, 'item_key_to_show');
	process_order = fn.getNestedElement(params, 'process_order');
	key_to_join = fn.getNestedElement(params, 'key_to_join');
	number_of_month = fn.getNestedElement(params, 'number_of_month', 1);
	start_month = fn.getNestedElement(params, 'start_month');
	custom_params = copy.deepcopy(params);
	result = {};
	main_po = {};
	
	df = pd.DataFrame(data[:]).astype(str);
	df = preprocessDataFrame(data=df);
	Logger.v('start_month', start_month);
	month_range = getMonthRange(params=custom_params);

	# print(df['approved_year_month']);
	functions = {
		'issue_quantity': sum, 
		'facility_name':'first', 
		'requester_name': 'first', 
		'item_name': 'first', 
		'sub_group_name': 'first', 
		'item_category_name': 'first', 
		'pku_name': 'first', 
		'drug_nondrug_name': 'first', 
		'requester_group_name': 'first', 
		'ptj_name': 'first',
		'approved_year_month': 'first',
		'issue_type': 'first',
		'item_group_name': 'first',
	}
	# df.info();
	# print(df['requester_group_name']);
	table = pd.pivot_table(df, values=list(functions.keys()), index=key_to_join, aggfunc=functions);
	new_table = table.reset_index(level=key_to_join);
	# print(new_table);
	# new_table.info();

	result = {};
	for index, row in new_table.iterrows():
		unique_list = [];
		for ktj in key_to_join:
			unique_list.append(row[ktj]);
		unique_id = '|'.join(unique_list);
		# Logger.v('row', row);
		# exit();
		codename = {
			'state': {
				'name': row['state'],
				'code': row['state'],
			},
			'facility_code': {
				'name': row['facility_name'],
				'code': row['facility_code'],
			},
			'requester_group_code': {
				'name': row['requester_group_name'],
				'code': row['requester_group_code'],
			},
			'drug_nondrug_code': {
				'name': row['drug_nondrug_name'],
				'code': row['drug_nondrug_code'],
			},
			'item_code': {
				'name': row['item_name'],
				'code': row['item_code'],
			},
		};
		for idx in range(0, len(process_order)):
			po = process_order[idx];
			if idx == 0:
				code = codename[po]['code'];
				name = codename[po]['name'];
				if code not in result:
					result[code] = {};
				temp_result = result[code];
				temp_result.update({
					'id': fn.convertToSnakecase(code),
					'name': name,
					'code': code,
				});
			elif idx == len(process_order) - 1:
				code = row['item_code'];
				name = row['item_name'];
				if po not in temp_result:
					temp_result.update({
						po: {},
					});
				if unique_id not in temp_result[po]:
					temp_result[po][unique_id] = {};

				temp_result = temp_result[po][unique_id];
				
				temp_result.update({
					'id': fn.convertToSnakecase(unique_id),
					'name': name,
					'code': code,
					'quantity': row['issue_quantity'],
					'quantity_by_month': {},
				});

				for mr in month_range:
					if mr == row['approved_year_month']:
						monthly_quantity = row['issue_quantity'];					
					else:
						monthly_quantity = 0;

					temp_result['quantity_by_month'].update({
						mr: monthly_quantity,
					});

				for item_show in item_key_to_show:
					# Logger.v('item show', item_show, row[item_show])
					temp_result.update({
						item_show: row[item_show],	
					});
			else:
				code = codename[po]['code'];
				name = codename[po]['name'];
				if po not in temp_result:
					temp_result.update({
						po: {},
					});
				if code not in temp_result[po]:
					temp_result[po][code] = {};
				temp_result = temp_result[po][code];
				
				temp_result.update({
					'id': fn.convertToSnakecase(code),
					'name': name,
					'code': code,
				});

	return result;

def preprocessDataFrame(data):
	## cast to number
	data['issue_quantity'] = pd.to_numeric(data['issue_quantity']);
	## sample data
	data['packaging'] = 'bottle of 0.5 litre';
	data['pku_code'] = 'bott';
	data['pku_name'] = 'bottle';
	data['requester_group_code'] = data['requester_group_name'];
	data['item_group_name'] = data['item_group'].apply(lambda x: 'drug' if x == 'd' else 'item');
	return data;

def getMonthRange(params):
	start_month = fn.getNestedElement(params, 'start_month');
	number_of_month = fn.getNestedElement(params, 'number_of_month', 1);
	month_range = [start_month];
	new_month = '{0}-01'.format(start_month);
	for month_count in range(0, number_of_month - 1): # included start_month, so total month less 1
		new_month = DateTime.getNextMonth(DateTime.convertDateTimeFromString(new_month));
		year_month = DateTime.getDateCategoryName(new_month, element='year_month_digit');
		month_range.append(year_month);
	return month_range;

def upload(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	date = fn.getNestedElement(params, 'date');
	path = fn.getNestedElement(params, 'path');
	# url = fn.getNestedElement(params, 'callback_url'); # required params to handle callback_url
	paths, should_reset = ModelUpload.getPath(params);
	for idx in range(0, len(paths)):
		p = paths[idx];
		processed_filename = File.converExcelFileToCsv(p, ignore_index=True);
		Logger.v('processed_filename', processed_filename);
		Debug.trace('convert to json : path {0}'.format( processed_filename ) );
		if idx == 0 and should_reset: #reset once at the beginning
			Logger.v('Reset Database.');
			reset(date); #reset stock_issue collection
			ModelSIIntegrity.reset(date); #reset stock_issue_datalog by date given
		File.readCsvFileInChunks(processed_filename, save, params, chunksize=chunksize);
		Debug.trace('uploaded to mongo.');
	generateIndex();
	ModelSIIntegrity.generateIndex();
	Debug.trace('indexing mongo collection.');
	saveIssueOption();
	Debug.trace('save option to json.');
	trigger_params = copy.deepcopy(params);
	trigger_params['result'] = 'data count: {0}'.format(params['data_count'][path]);
	# Logger.v('trigger_params', trigger_params);
	dbManager.executeBulkOperations(None); # Insert all the remaining job at once.
	ReportStock.triggerOnComplete(trigger_params);
	Debug.trace('trigger api on complete.');
	Debug.end();
	Debug.show('Stock.upload');

@asyncio.coroutine
def save(params, chunk, chunks_info):
	global collection_name, column_keymap;
	upload_date = fn.getNestedElement(params, 'date');
	data = File.readChunkData(chunk);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	current_index = fn.getNestedElement(chunks_info, 'current', 0);
	total_index = fn.getNestedElement(chunks_info, 'total', len(data));

	total_length = len(data);
	queue_info = chunks_info['queue']
	# Logger.v('Running Index:', chunks_info['queue']['running']);
	chunks_info['queue']['current']+=1;
	# Logger.v('Saving from... {0}/{1}, current package: {2}'.format(current_index, total_index, total_length) );
	fn.printProgressBar(queue_info['current'], queue_info['total'], 'Processing Chunk Insertion');
	for idx in range(0, total_length):
		row = data[idx];
		# Logger.v('row', row);
		obj_ = transformToLowercase(row);
		date_only = obj_['approved_date'].split(' ')[0];
		# Logger.v('date_only', date_only);
		obj_.update({
			'approved_year_month': DateTime.getDateCategoryName(date=date_only, element='year_month_digit'),
			'upload_date': upload_date,
		});
		dbManager.addBulkInsert(collection_name, obj_, batch=True);
		ModelSIIntegrity.update(data=obj_);
		retrieveIssueOption(obj_);
	#ensure all data is save properly
	dbManager.executeBulkOperations(collection_name);
	return chunks_info;

def transformToLowercase(data):
	global column_keymap;
	# key to snakecase, value to lowercase
	# Logger.v('data', data)
	obj_ = {};
	for row_key in data:
		row_value = data[row_key];
		# Logger.v('key', row_key, 'value', row_value);
		# new_key = row_key.replace(' ', '_').lower(); # use 3MB data for fast testing
		new_key = column_keymap[row_key].replace(' ', '_').lower(); # latest data require mapping
		if type(row_value) == str:
			new_value = row_value.lower();
		else:
			new_value = row_value;
		obj_[new_key] = new_value;
	return obj_;

def generateIndex():
	global collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	db[collection_name].create_index([('approved_year_month', -1), ('drug_nondrug_code', 1), ('state_name', 1), ('facility_code', 1), ('requester_group_name', 1)]);

def saveIssueOption():
	global stock_issue_options;
	result = {};
	keys = ['state', 'requester_group', 'issue_type'];
	for key in keys:
		options = copy.deepcopy(stock_issue_options[key]);
		if key in ['requester_group', 'issue_type']: # include 'all' option
			options += ['all'];
		result[key] = sorted(list(set(options)));
	result['available_month'] = checkAvailableMonth();
	filename = '{0}/stock_issue_options.json'.format(reference_folder);
	fn.writeJSONFile(filename=filename, data=result);

def retrieveIssueOption(data):
	global stock_issue_options;
	if 'state' not in stock_issue_options:
		stock_issue_options['state'] = [];
	if 'requester_group' not in stock_issue_options:
		stock_issue_options['requester_group'] = [];
	if 'issue_type' not in stock_issue_options:
		stock_issue_options['issue_type'] = [];

	state = fn.getNestedElement(data, 'state');
	if state:
		stock_issue_options['state'].append(state);

	requester_group = fn.getNestedElement(data, 'requester_group_name');
	if requester_group:
		stock_issue_options['requester_group'].append(requester_group);

	issue_type = fn.getNestedElement(data, 'issue_type');
	if issue_type:
		stock_issue_options['issue_type'].append(issue_type);

	return stock_issue_options;

def checkAvailableMonth():
	limit = 6;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	data = list(db[collection_name].find({}, {'_id':0, 'approved_year_month': 1}));
	df = pd.DataFrame(data);
	months = df['approved_year_month'].sort_values(ascending=False).unique().tolist();
	return months[:limit];

def reset(date):
	global collection_name, stock_issue_options;
	stock_issue_options = {};
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	query = {
		'upload_date': date,
	};
	db[collection_name].delete_many(query);