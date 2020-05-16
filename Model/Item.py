import asyncio;
import re;
from bson import regex
from lib import fn, Logger, File, SharedMemoryManager, Params;

collection_name = 'items';
attributes_unique = ['Item Code', 'SKU'];
attributes_search = ['Item Desc', 'Sub Group Desc', 'Item Category Desc', 'Item Group'];
SEARCH_LIMIT = int(fn.getNestedElement(fn.config, 'SEARCH_LIMIT', '200'));
attributes_keymap = {
	'PTJ Code': 'PTJ_CODE',
	'PTJ Name': 'PTJ_NAME',
	'Facility Code': 'FACILITY_CODE',
	'Facility Name': 'FACILITY_NAME',
	'Requester Unit Code': 'DES_RQSTR_CODE',
	'Requester Unit Desc': 'DES_RQSTR_DESC',
	'Item Code': 'DES_ITEM_CODE',
	'Item Desc': 'DES_ITEM_DESC',
	'Available Quantity': 'AVAILABLE_QTY',
	'Unit Price': 'DES_AVG_PRICE',
	'SKU': 'DES_SKU_UOM_CODE',
	'Batch No': 'DES_BATCH_NO',
	'Expiry Date': 'DES_BATCH_EXPIRY_DATE',
	'Sub Group Code': 'DES_ITEM_SUBGROUP_CODE',
	'Sub Group Desc': 'DES_ITEM_SUBGROUP_DESC',
	'Item Category Code': 'DES_ITEM_CAT_CODE',
	'Item Category Desc': 'DES_ITEM_CAT_DESC',
	'Item Group': 'DES_ITEM_GROUP',
	'PKU Code': 'DES_PKU_UOM_CODE',
	'PKU Desc': 'DES_PKU_UOM_DESC',
	'Drug Nondrug Code': 'DES_DRUG_NONDRUG_CODE',
	'Drug Nondrug Desc': 'DES_DRUG_NONDRUG_DESC',
	'Packaging': 'DES_PACKAGING',
	'Requester Group Code': 'DES_RQSTR_GROUP_CODE',
	'Requester Group Name': 'DES_RQSTR_GROUP_NAME',
};
# def upload(params):
def upload(filename, new=True):
	dbManager = SharedMemoryManager.getInstance();
	processed_filename = File.converExcelFileToCsv(filename, ignore_index=True);
	if processed_filename:
		if new:
			reset(); #reset Database.
		File.readCsvFileInChunks(processed_filename, save, {}, chunksize=1000);
		dbManager.executeBulkOperations(collection_name);
	else:
		Logger.e('File does not exists: ', filename);

def load(filename, callback=None):
	processed_filename = File.converExcelFileToCsv(filename, ignore_index=True);
	# File.readCsvFileInChunks(processed_filename, test);
	data = File.convertCsvFileToDict(processed_filename);
	##preprocess here
	return data;
def reset():
	global inserted;
	inserted = [];
	dbManager = SharedMemoryManager.getInstance();
	# if total_length> 0: # reset database everyday.
	new_name = '{0}_drop'.format(collection_name);
	dbManager.renameCollection(collection_name, new_name);
	dbManager.dropCollection(new_name);

def extractRow(row):
	global attributes_unique, attributes_search, attributes_keymap;
	key = {};
	result = {};
	for attribute in attributes_unique:
		key[attribute] = row[attributes_keymap[attribute]];
		result[attribute] = row[attributes_keymap[attribute]];

	search_text = []; 
	for attribute in attributes_search:
		file_column_key = attributes_keymap[attribute];
		if file_column_key in row:
			result[attribute] = row[file_column_key];
		search_text.append(row[file_column_key]);
	result['search_text'] = " ".join(str(v) for v in search_text);


	unique_key = "-".join(str(v) for v in key.values());
	return unique_key, result;


inserted = [];
@asyncio.coroutine
def save(params, chunk, chunks_info):
	global collection_name;
	dbManager = SharedMemoryManager.getInstance();
	data = File.readChunkData(chunk);
	total_length = len(data);
	queue_info = chunks_info['queue']
	# Logger.v('Running Index:', chunks_info['queue']['running']);
	chunks_info['queue']['current']+=1;
	# Logger.v('Saving from... {0}/{1}, current package: {2}'.format(current_index, total_index, total_length) );
	fn.printProgressBar(queue_info['current'], queue_info['total'], 'Processing Chunk Insertion');

	for idx in range(0, total_length):
		d = data[idx];
		saveItem(d)
		# fn.printProgressBar(idx, total_length, 'Processing Item Search');
	dbManager.executeBulkOperations(collection_name);

def saveItem(row):
	global collection_name, inserted;
	dbManager = SharedMemoryManager.getInstance();
	unique_key, row = extractRow(row);
	if unique_key in inserted:
		return;
	dbManager.addBulkInsert(collection_name, row, batch=True);
	inserted.append(unique_key);
	dbManager.executeBulkOperations(collection_name);


def getDBKey(params_key):
	global attributes_unique, attributes_search;
	attributes = attributes_unique + attributes_search;
	result = {};
	for attribute in attributes:
		key = attribute.lower().replace(' ', '_');
		if key == params_key:
			return attribute;
	return None;

def generateUniqueWordList(words, sep=' '):
	keyword_list = [];
	exact_words = re.findall(r'"([^"]*)"', words);# extract exact word within double quotes
	for word in exact_words: 
		keyword_list.append(word);
		words = words.replace('"{0}"'.format(word), ''); # remove the exact word from query

	for x in ['\'','\"','(',')','*']: # remove special character
		words = words.replace(x, '');
	unique_word_list = [ x.strip() for x in set(words.split(sep)) ]; # split by seperator and trim it
	unique_word_list = list(filter(lambda x: len(x.strip(' ')) >=2, unique_word_list) );  #remove the empty 1.
	return keyword_list + unique_word_list;

def getQuery(params):
	global attributes_unique, attributes_search;
	attributes = attributes_unique + attributes_search + ['search_text'];
	result = {};
	for attribute in attributes:
		key = attribute.lower().replace(' ', '_');
		if key in params:
			Logger.v('{0}: {1}'.format(key, params[key]));
			if key == 'search_text':
				conditions = [];
				unique_word_list = generateUniqueWordList(params[key], ' ');
				for attr in attributes_search:
					regex_conditions = [];
					for word in unique_word_list:
						regex_conditions.append({ attr:{ '$regex': word, '$options': 'i' }});
					conditions.append({'$and':regex_conditions});
				result['$or'] = conditions;
			elif key == 'item_code':
				conditions = [];
				unique_word_list = generateUniqueWordList(params[key], ',');
				result[attribute] = {'$in': unique_word_list };
			
			elif type(params[key]) == str:
				result[attribute] =  { '$regex': params[key], '$options': 'i' } ;
			elif type(params[key]) == list:
				result[attribute] = {'$in': params[key] };
			else:
				result[attribute] = {'$eq': params[key] };
	return result;

def processItemList(data, group_by):
	listed = [];
	result = [];
	for d in data:
		row = {};
		for old_key in d:
			key = old_key.lower().replace(' ', '_');
			row[key] = str(fn.getNestedElement(d, old_key,''));
		item_code = fn.getNestedElement(row, 'item_code','');
		if not item_code in listed:
			listed.append(item_code);
			result.append(row);
	# if group_by:
	# 	row['id'] =
	if len(result) > SEARCH_LIMIT: 
		return result[:SEARCH_LIMIT]; # limit top 200 result only.
	return result;

def search(params):
	global collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();

	group_by = fn.getNestedElement(params, 'group_by', None);
	query = getQuery(params);

	aggregate_query = [];
	aggregate_query.append({ #key searching query
			'$match': query
		});
	if group_by:
		group_by_key = '$'+getDBKey(group_by);
		aggregate_query.append({ '$unwind': group_by_key });
		aggregate_query.append({ #key searching query
			'$group': { '_id': group_by_key, "name": { '$first':group_by_key }, "count": { "$sum": 1},  }
		});
		
	aggregate_query.append({ # key to remove.
			'$project': {'_id': 0, 'updated_at':0, 'inserted_at':0}
		});
	Logger.v('Query:',aggregate_query);
	# fn.show(aggregate_query);
	data = db[collection_name].aggregate(aggregate_query);
	data = list(data);
	return data;
