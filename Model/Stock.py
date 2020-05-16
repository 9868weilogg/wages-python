from lib import SharedMemoryManager;
from lib import DebugManager;
from lib import fn;
from lib import Logger;
from lib import DateTime;
from lib import File;
from Model import StockIntegrity as ModelStockIntegrity;
from Model import Facility as ModelFacility;
from Model import Item as ModelItem;
from Model import Upload as ModelUpload;
from Report import Stock as ReportStock;

from pymongo import TEXT;
import pandas as pd;
import json;
import copy;
import requests;
import asyncio;
import pytz;
import math;

# latest_collection_name = 'stock_30'; # TEST
latest_collection_name = 'stock_latest';
history_collection_name = 'stock';
query_key = {
	'state': 'state',
	'facility': 'facility_code',
	'requester': 'requester_unit_code',
	'drug': 'item_code',
};
naming_keymap = {
	'state': 'state',
	'facility_code': 'facility_name',
	'item_code': 'item_desc',
	'requester_unit_code': 'requester_unit_desc',
	'sub_group_code': 'sub_group_desc',
	'item_category_code': 'item_category_desc',
	'pku_code': 'pku_desc',
	'drug_nondrug_code': 'drug_nondrug_desc',
	'requester_group_code': 'requester_group_name',
	'unit_price': 'unit_price',
	'item_group': 'item_group', 
	'ptj_code': 'ptj_name', 
};
column_keymap = {
	'STATE_NAME': 'State',
	'PTJ_CODE': 'PTJ Code',
	'PTJ_NAME': 'PTJ Name',
	'FACILITY_CODE': 'Facility Code',
	'FACILITY_NAME': 'Facility Name',
	'DES_RQSTR_CODE': 'Requester Unit Code',
	'DES_RQSTR_DESC': 'Requester Unit Desc',
	'DES_ITEM_CODE': 'Item Code',
	'DES_ITEM_DESC': 'Item Desc',
	'AVAILABLE_QTY': 'Available Quantity',
	'DES_AVG_PRICE': 'Unit Price',
	'DES_SKU_UOM_CODE': 'SKU',
	'DES_BATCH_NO': 'Batch No',
	'DES_BATCH_EXPIRY_DATE': 'Expiry Date',
	'DES_ITEM_SUBGROUP_CODE': 'Sub Group Code',
	'DES_ITEM_SUBGROUP_DESC': 'Sub Group Desc',
	'DES_ITEM_CAT_CODE': 'Item Category Code',
	'DES_ITEM_CAT_DESC': 'Item Category Desc',
	'DES_ITEM_GROUP': 'Item Group',
	'DES_PKU_UOM_CODE': 'PKU Code',
	'DES_PKU_UOM_DESC': 'PKU Desc',
	'DES_DRUG_NONDRUG_CODE': 'Drug Nondrug Code',
	'DES_DRUG_NONDRUG_DESC': 'Drug Nondrug Desc',
	'DES_PACKAGING': 'Packaging',
	'DES_RQSTR_GROUP_CODE': 'Requester Group Code',
	'DES_RQSTR_GROUP_NAME': 'Requester Group Name',

};
extra_columns = {
	'all': ['facility_code', 'requester_unit_code'],
	'state': ['facility_code', 'requester_unit_code'],
	'facility': ['facility_code'],
	'requester': ['facility_code', 'requester_unit_code', 'sub_group_code', 'item_category_code', 'pku_code', 'drug_nondrug_code', 'requester_group_code', 'ptj_code'],
	'drug': ['facility_code', 'requester_unit_code', 'sub_group_code', 'item_category_code', 'pku_code', 'drug_nondrug_code', 'requester_group_code', 'ptj_code'],
}
project_root_folder = fn.getRootPath();
crawl_folder = '{0}/crawled_data/stock'.format(project_root_folder);
msia_tz = pytz.timezone('Asia/Kuala_Lumpur');
date_retrieve_limit = 7;
chunksize = int(fn.getNestedElement(fn.config, 'UPLOAD_CHUNK_SIZE','2500'));

def get(params):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();

	data= [];
	getCollectionName(params);
	query = getQuery(params);
	Logger.v('Get data from collection:', latest_collection_name);
	data = list(db[latest_collection_name].aggregate([
		{
			'$match': query
		},
		{
			'$project': {'_id': 0, 'inserted_at': 0, 'updated_at': 0}
		}
	]));
	Logger.v('data length', len(data));
	return data;

def getQuery(params):
	duration = fn.getNestedElement(params, 'duration', ['2020-03-30', '2020-03-30']);
	item_codes = fn.getNestedElement(params, 'item_codes', []);
	item_desc = fn.getNestedElement(params, 'item_desc');
	group_by_list = fn.getNestedElement(params, 'group_by', []);
	facility_group = fn.getNestedElement(params, 'facility_group', []);

	dates = DateTime.getBetween(duration, element='date')['order'];
	query = {};
	if item_desc: # TEST wildcard search
		query.update({
			'item_desc': {'$regex': item_desc.lower()}
		});
	if item_codes:
		query.update({
			'item_code': {'$in': [c.lower() for c in item_codes]},
		});
	if facility_group:
		facility_code_list = ModelFacility.getFacilityCodeList(facility_group=facility_group);
		query.update({
			'facility_code': {'$in': facility_code_list},
		});

	for gbl in group_by_list:
		gbl_id = gbl['id'];
		gbl_value = gbl['value'];
		val = gbl_value;
		if type(val) == str:
			val = val.lower();
			if gbl_id == 'state':
				val = val.replace('_', ' ');
		query.update({
			query_key[gbl_id]: val,
		});
	Logger.v('query', query);
	return query;

def getCollectionName(params):
	global latest_collection_name;
	latest_collection_name = 'stock_latest'; # set default;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	data = list(db[latest_collection_name].find({}, {'_id': 0, 'date': 1}).limit(1));
	if data:
		latest_date_string = DateTime.toString(data[0]['date']);
		latest_date = DateTime.convertDateTimeFromString(latest_date_string);
		date_string = fn.getNestedElement(params, 'date', None);
		if date_string:
			date = DateTime.convertDateTimeFromString(date_string);
			different = latest_date - date;
			day_diff = math.floor(different.total_seconds()/float(86400));
			if day_diff > 0:
				latest_collection_name = 'stock_{0}'.format(day_diff);

	# 	Logger.v('date', date, 'latest_date', latest_date, 'diff', day_diff, 'latest_collection_name', latest_collection_name);
	# Logger.v('latest_collection_name', latest_collection_name);
	# exit();

def quantityWithinRange(params, quantity):
	filter_quantity = fn.getNestedElement(params, 'filter.quantity', []);
	can_append = False;
	can_append_count = 0;
	for fq in filter_quantity:
		filter_mode = fn.getNestedElement(fq ,'id');
		filter_value = fn.getNestedElement(fq, 'value');
		if filter_mode == 'lte':
			ok = quantity <= filter_value;
		elif filter_mode == 'lt':
			ok = quantity < filter_value;
		elif filter_mode == 'gte':
			ok = quantity >= filter_value;
		elif filter_mode == 'gt':
			ok = quantity > filter_value;

		if ok:
			can_append_count += 1;

		if can_append_count == len(filter_quantity):
			can_append = True;
	return can_append;

def calculateData(params, data):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');
	global naming_keymap, crawl_folder;
	item_key_to_show = fn.getNestedElement(params, 'item_key_to_show');
	process_order = fn.getNestedElement(params, 'process_order');
	custom_params = copy.deepcopy(params);
	result = {};
	main_po = {};
	
	df = pd.DataFrame(data[:]).astype(str);
	df = preprocessDataframe(params=custom_params, data=df);
	summation_df = groupDataframe(params=custom_params, data=df);

	# to check summation result
	# output_file = '{0}/output/find_result.xlsx'.format(crawl_folder);
	# df.to_excel(output_file);
	Debug.trace('dataframe process');

	for idx in range(0, len(process_order)):
		main_po[idx] = {
			'po': process_order[idx],
			'group_po': [],
		};
		for idx1 in range(0, idx+1):
			main_po[idx]['group_po'].append(process_order[idx1]);

	for idx in range(0, len(process_order)):
		po = main_po[idx]['po'];
		group_po = main_po[idx]['group_po'];
		custom_params['po'] = {
			'po': po,
			'naming_keymap': naming_keymap,
		};
		grouped_df = df.groupby(group_po).groups;
		Logger.v('len', idx, 'th', len(grouped_df.keys()))
		if idx == 0:
			for gk in grouped_df.keys():
				code = gk.split('|')[-1];
				name = df[df[po] == gk][naming_keymap[po]].unique().tolist()[0];
				result[gk] = {
					'id': fn.convertToSnakecase(gk),
					'name': name,		
					'code': code,
				};
		else:
			for gk in grouped_df.keys():
				level = gk[-2];
				code = gk[-1];

				if idx == 1:
					temp_result = result;
				elif idx == 2:
					# Logger.v('gk0', gk[0], main_po[idx-1]['po'], 'level', level, 'code', code);
					temp_result = fn.getNestedElement(result, '{0}.{1}'.format(gk[0], main_po[idx-1]['po']));
				elif idx == 3:
					# Logger.v('gk0', gk[0], main_po[idx-2]['po'], gk[1], main_po[idx-1]['po'], 'level', level, 'code', code);
					temp_result = fn.getNestedElement(result, '{0}.{1}.{2}.{3}'.format(gk[0], main_po[idx-2]['po'], gk[1], main_po[idx-1]['po']));

				if po not in temp_result[level]:
					temp_result[level][po] = {};
				if code not in temp_result[level][po]:
					temp_result[level][po][code] = {};

				custom_params['po'].update({
					'gk': code,
				});
				# when this is the last element in process_order
				if process_order[-1] == po:
					last_child_data = insertNthChild(params=custom_params, data=summation_df, is_last=True);
					info = last_child_data['info'];
					temp_result[level][po][code] = last_child_data['obj_'];
					# add extra info by group_by
					for ik in item_key_to_show:
						temp_result[level][po][code].update({
							ik: info[ik].values[0],
						});
				else:
					last_child_data = insertNthChild(params=custom_params, data=summation_df);
					temp_result[level][po][code] = last_child_data['obj_'];
		Debug.trace('{0}th'.format(idx));

	Debug.end();
	Debug.show('Model.Stock.calculateData');
	return result;

def preprocessDataframe(params, data):
	key_to_join = fn.getNestedElement(params, 'key_to_join');
	processed_df = copy.deepcopy(data);
	joined_key = [];
	joined_ = [];
	# sample data
	processed_df['pku_code'] = 'bott';
	processed_df['pku_desc'] = 'bottle';
	processed_df['drug_nondrug_code'] = 'D08AC52137L9901XX'.lower();
	processed_df['drug_nondrug_desc'] = 'Chlorhexidine 1:200 in Alcohol with Emollient (for hand disinfection)'.lower();
	processed_df['packaging'] = 'bottle of 0.5 Litre'.lower();
	processed_df['requester_group_code'] = 'Unit/Ward'.lower();
	processed_df['requester_group_name'] = 'Unit/Ward'.lower();

	for idx in range(0, len(key_to_join)):
		ktj = key_to_join[idx];
		joined_key.append(ktj);
		if idx > 0:
			joined_.append(['_'.join(joined_key[:-1]), ktj]);
			columns = joined_[idx-1];
			joined_columns = '_'.join(columns);
			processed_df[joined_columns] = processed_df[columns[0]].str.cat(processed_df[columns[1]], sep ="|");

	processed_df['available_quantity'] = pd.to_numeric(processed_df['available_quantity']);
	processed_df['unit_price'] = pd.to_numeric(processed_df['unit_price']);
	processed_df['pku'] = processed_df['available_quantity'] * processed_df['unit_price'];
	processed_df['full_facility_name'] = processed_df['facility_name'];
	processed_df['facility_name'] = processed_df['facility_name'].map(ModelFacility.facilityNameShorten);

	return processed_df;

def groupDataframe(params, data):
	global naming_keymap;
	process_order = fn.getNestedElement(params, 'process_order');
	key_to_join = fn.getNestedElement(params, 'key_to_join');
	last_key = '_'.join(key_to_join);
	summation_df = data.groupby(last_key)[['available_quantity', 'pku']].apply(sum).reset_index().sort_values('available_quantity', ascending=True);
	for idx in range(0, len(key_to_join)):
		key = key_to_join[idx];
		summation_df[key] = summation_df[last_key].str.split(pat='|', expand=True)[idx];

	extra_cols = getCodeColumn(params);

	# map code to name of (facility, item, requester_unit)
	for ec in extra_cols:
		# Logger.v('ec', ec, naming_keymap[ec]);
		ec_df = data[[ec, naming_keymap[ec]]].drop_duplicates();
		mapping_dict = dict(zip(ec_df[ec].tolist(), ec_df[naming_keymap[ec]].tolist()));
		summation_df[naming_keymap[ec]] = summation_df[ec].map(mapping_dict);
	return summation_df;

def getCodeColumn(params):
	global extra_columns;
	process_order = fn.getNestedElement(params, 'process_order');
	group_by_key = fn.getNestedElement(params, 'group_by_key');
	extra_col = copy.deepcopy(process_order[:-1]);
	extra_col += extra_columns[group_by_key];
	if 'item_code' not in extra_col:
		extra_col.append('item_code');
	if 'state' in extra_col:
		extra_col.remove('state');
	return extra_col;

def insertNthChild(params, data, is_last=False):
	po = fn.getNestedElement(params, 'po.po');
	gk = fn.getNestedElement(params, 'po.gk');
	naming_keymap = fn.getNestedElement(params, 'po.naming_keymap');
	info = data[data[po] == gk];

	if is_last:
		obj_ = {
			'id': fn.convertToSnakecase(gk),
			'name': info['item_desc'].values[0],
			'code': info['item_code'].values[0],
			'quantity': int(info['available_quantity'].values[0]),
			'pku': round(float(info['pku'].values[0]), 2),
		};
		
	else:
		obj_ = {
			'id': fn.convertToSnakecase(gk),
			'name': info[naming_keymap[po]].unique().tolist()[0],
			'code': info[po].unique().tolist()[0],
		};

	result = {
		'obj_': obj_,
		'info': info,
	};
	return result;

def backdateCollection(days=date_retrieve_limit):
	global latest_collection_name;
	dbManager = SharedMemoryManager.getInstance();
	for idx in range(days, 0, -1):
		collection_names = dbManager.getCollectionNames();
		col_name = 'stock_{0}'.format(idx);
		if idx > 1:
			previous_col_name = 'stock_{0}'.format(idx-1);
		else:
			previous_col_name = latest_collection_name;

		if col_name in collection_names:
			dbManager.dropCollection(col_name);

		if previous_col_name in collection_names:
			Logger.v('rename', previous_col_name, 'to', col_name);
			dbManager.renameCollection(previous_col_name, col_name);
		else:
			Logger.v('create', col_name);
			dbManager.createCollection(col_name);
	dbManager.createCollection(latest_collection_name);

def reset():
	global latest_collection_name;
	dbManager = SharedMemoryManager.getInstance();
	new_name = '{0}_drop'.format(latest_collection_name);
	dbManager.renameCollection(latest_collection_name, new_name);
	dbManager.dropCollection(new_name);

def generateIndex():
	global latest_collection_name, history_collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	for collection_name in [latest_collection_name, history_collection_name]:
		db[collection_name].create_index([('date', -1), ('item_code', 1), ('state', 1), ('facility_code', 1), ('requester_unit_code', 1)]);
		db[collection_name].create_index([('item_desc', TEXT)], default_language='english');

@asyncio.coroutine
def save(params, chunk, chunks_info):
	global latest_collection_name, history_collection_name;

	data = File.readChunkData(chunk);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	current_index = fn.getNestedElement(chunks_info, 'current', 0);
	total_index = fn.getNestedElement(chunks_info, 'total', len(data));

	date = fn.getNestedElement(params, 'date');
	datetime = DateTime.convertDateTimeFromString(date);
	total_length = len(data);
	queue_info = chunks_info['queue']
	# Logger.v('Running Index:', chunks_info['queue']['running']);
	chunks_info['queue']['current']+=1;
	# Logger.v('Saving from... {0}/{1}, current package: {2}'.format(current_index, total_index, total_length) );
	fn.printProgressBar(queue_info['current'], queue_info['total'], 'Processing Chunk Insertion');
	for idx in range(0, total_length):
		# insert stock_latest
		row = data[idx];
		obj_ = transformToLowercase(data=row, datetime=datetime);
		ModelStockIntegrity.update(data=obj_);
		dbManager.addBulkInsert(latest_collection_name, obj_, batch=True);
		# dbManager.addBulkInsert(history_collection_name, obj_, batch=True); # temporary off (need 7 day data only)

		# insert items
		# d = data[idx];
		ModelItem.saveItem(row);
		# fn.printProgressBar(current_index+idx, total_index, 'Processing Item Insertion');
	
	#ensure all data is save properly
	# dbManager.executeBulkOperations(history_collection_name); # temporary off (need 7 day data only)
	dbManager.executeBulkOperations(latest_collection_name);
	return chunks_info;

def transformToLowercase(data, datetime):
	global column_keymap;
	# key to snakecase, value to lowercase
	obj_ = {
		'date': datetime
	};
	for row_key in data:
		row_value = data[row_key];
		# new_key = row_key.replace(' ', '_').lower(); # use 3MB data for fast testing
		new_key = column_keymap[row_key].replace(' ', '_').lower(); # latest data require mapping
		if type(row_value) == str:
			new_value = row_value.lower();
		else:
			new_value = row_value;
		obj_[new_key] = new_value;
	return obj_;

def getBackdateList(params):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	dates = [];
	for idx in range(0, date_retrieve_limit+1): # 7 days backward + 1 today
		if idx == 0:
			collection_name = 'stock_latest';
		else:
			collection_name = 'stock_{0}'.format(idx);
		# Logger.v('collection_name', collection_name);
		data = list(db[collection_name].find({}, {'_id': 0, 'date': 1}).limit(1));
		if data:
			date = DateTime.toString(data[0]['date']);
			# Logger.v('date', date);
			dates.append(date);
		# Logger.v('data', data);

	# Logger.v('dates', sorted(list(set(dates)), reverse=True));
	result = {'date': sorted(list(set(dates)), reverse=True)};
	return result;

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
			# ModelUpload.inserted = [];
			reset(); #reset stock_latest collection
			ModelItem.reset(); #reset items collection
			ModelStockIntegrity.reset(date); #reset stock_datalog by date given
		File.readCsvFileInChunks(processed_filename, save, params, chunksize=chunksize);
		Debug.trace('uploaded to mongo.');
	generateIndex();
	ModelStockIntegrity.generateIndex();
	Debug.trace('indexing mongo collection.');
	trigger_params = copy.deepcopy(params);
	trigger_params['result'] = 'data count: {0}'.format(params['data_count'][path]);
	dbManager.executeBulkOperations(None); # Insert all the remaining job at once.
	ReportStock.triggerOnComplete(trigger_params);
	Debug.trace('trigger api on complete.');
	Debug.end();
	Debug.show('Stock.upload');