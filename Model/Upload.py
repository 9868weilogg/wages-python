import asyncio;

from lib import SharedMemoryManager;
from lib import DateTime;
from lib import Logger;
from lib import fn;
from lib import File;
from Model import Stock as ModelStock;
from Model import StockIntegrity as ModelStockIntegrity;
from Model import Item as ModelItem;
from Report import Item;

upload_log_collection_name = 'upload_log';

def updateLog(params):
	global upload_log_collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	date = fn.getNestedElement(params, 'date');
	path = fn.getNestedElement(params, 'path');
	group = fn.getNestedElement(params, 'group');
	data_part = fn.getNestedElement(params, 'data_part', 'default');
	if type(group) == str:
		group = group.lower();
	if type(data_part) == str:
		data_part = data_part.lower();
	query = {
		'date': date,
		'collection': group,
	};
	stock_upload_log = list(db[upload_log_collection_name].find(query, {'_id': 0, 'inserted_at': 0, 'updated_at': 0}));
	# Logger.v('stock_upload_log', stock_upload_log);
	if not stock_upload_log and group == 'stock':
		# Logger.v('backdate collection');
		ModelStock.backdateCollection();

	# Logger.v('update upload_log collection');
	values = {};
	if stock_upload_log:
		if 'part_of_the_day' not in values:
			values['part_of_the_day'] = [];

		for part in stock_upload_log[0]['part_of_the_day']:
			# Logger.v('part', part);
			values['part_of_the_day'].append(part);
			values[part] = stock_upload_log[0][part];
		if data_part not in stock_upload_log[0]['part_of_the_day']:
			values['part_of_the_day'].append(data_part);
			values[data_part] = path;

	else:
		values['part_of_the_day'] = [data_part];
		values[data_part] = path;
	# Logger.v('query', query, values)
	# exit();
	dbManager.addBulkUpdate(upload_log_collection_name, query, values, upsert=True, batch=False);

def getPath(params):
	global upload_log_collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	date = fn.getNestedElement(params, 'date');
	path = fn.getNestedElement(params, 'path');
	group = fn.getNestedElement(params, 'group');
	data_part = fn.getNestedElement(params, 'data_part', 'default');
	if type(group) == str:
		group = group.lower();
	if type(data_part) == str:
		data_part = data_part.lower();

	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	query = {
		'date': date,
		'collection': group,
	};
	stock_upload_log = list(db[upload_log_collection_name].find(query, {'_id': 0, 'inserted_at': 0, 'updated_at': 0}));
	# Logger.v('stock_upload_log', stock_upload_log);
	part_of_the_day = [];
	if stock_upload_log:
		part_of_the_day = stock_upload_log[0]['part_of_the_day'];
		paths = [];
		for part in part_of_the_day:
			paths.append(stock_upload_log[0][part]);
	updateLog(params);
	should_reset = True;
	if not data_part in part_of_the_day:
		should_reset = False if part_of_the_day else True;
		paths = [path];
	Logger.v('Upload paths:', paths, should_reset);
	return paths, should_reset;
