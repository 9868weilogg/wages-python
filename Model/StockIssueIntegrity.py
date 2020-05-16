import pytz;
import asyncio;

from lib import SharedMemoryManager;
from lib import DateTime;
from lib import Logger;
from lib import fn;
from lib import File;
from Model import Stock as ModelStock;
from Model import Item as ModelItem;
from Report import Item;

collection_name = 'stock_issue_datalog';
project_root_folder = fn.getRootPath();
crawl_folder = '{0}/crawled_data/stock'.format(project_root_folder);
msia_tz = pytz.timezone('Asia/Kuala_Lumpur');
date_retrieve_limit = 7;
date_count = 0;
unique_facility = [];

def check(params):
	global msia_tz, date_retrieve_limit, date_count, collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	today = DateTime.now(tzinfo=msia_tz);
	start_date = DateTime.getDaysAgo(date_retrieve_limit, datefrom=today);
	durations = DateTime.getBetween([start_date, today], element='date', offset=24)['order']; # offset 24 to include today
	Logger.v('durations', durations);
	data = db[collection_name].aggregate([
		{
			'$match': {'state_updated_at': {'$in': durations}, 'facility_updated_at': {'$in': durations} }
		},
		{
			'$project': {'_id': 0, 'inserted_at': 0, 'updated_at': 0}
		}
	]);
	data = list(data);
	Logger.v('Total stock issue integrity in', date_retrieve_limit, 'days:', len(data));
	state_data = {};
	facility_data_by_state = {};

	for idx in range(0, len(data)):
		row = data[idx];
		state_code = fn.getNestedElement(row, 'state_code');
		if state_code not in facility_data_by_state:
			facility_data_by_state[state_code] = {};

		state_data = addIntegrityData(data={'row':row, 'to_update': state_data}, category='state');
		facility_data_by_state[state_code] = addIntegrityData(data={'row':row, 'to_update': facility_data_by_state[state_code]}, category='facility');

		if date_count > date_retrieve_limit:  # limit loop data/ show data in N days
			break;
		date_count = 0; # reset to 0th day
	return {
		'state': state_data,
		'state_facility': facility_data_by_state,
	};

def addIntegrityData(data, category):
	global date_count;
	row = fn.getNestedElement(data, 'row');
	result = fn.getNestedElement(data, 'to_update');
	category_data_map = {
		'state': {
			'code': str(fn.getNestedElement(row, 'state_code')),
			'name': fn.getNestedElement(row, 'state_name'),
			'updated_at': fn.getNestedElement(row, 'state_updated_at'),
			'unique_code': fn.convertToSnakecase(fn.getNestedElement(row, 'state_code')),
		},
		'facility': {
			'code': str(fn.getNestedElement(row, 'facility_code')),
			'name': fn.getNestedElement(row, 'facility_name'),
			'updated_at': fn.getNestedElement(row, 'facility_updated_at'),
			'unique_code': fn.convertToSnakecase('_'.join([str(fn.getNestedElement(row, 'state_code')), str(fn.getNestedElement(row, 'facility_code'))])),
		},
	}
	code = fn.getNestedElement(category_data_map, '{0}.code'.format(category));
	name = fn.getNestedElement(category_data_map, '{0}.name'.format(category));
	updated_at = fn.getNestedElement(category_data_map, '{0}.updated_at'.format(category));
	unique_code = fn.getNestedElement(category_data_map, '{0}.unique_code'.format(category));
	if code not in result:
		result[code] = {
			'id': unique_code,
			'name': name,
			'code': code,
			'count': 1,
		};
	else:
		result[code]['count'] += 1;

	if updated_at not in result[code]:
		result[code][updated_at] = 0;
		if category == 'state':
			date_count += 1; # limit loop data/ show data in N days

	result[code][updated_at] += 1;
	return result;

def reset(date):
	global unique_facility;
	unique_facility = [];
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	query = {
		'date': date,
	};
	db[collection_name].delete_many(query);

def update(data):
	global collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	state_facility_code = '_'.join([str(data['state']), str(data['facility_code'])]);
	if state_facility_code not in list(set(unique_facility)):
		state_name = fn.getNestedElement(data, 'state');
		state_code = fn.getNestedElement(data, 'state');
		facility_name = fn.getNestedElement(data, 'facility_name');
		facility_code = fn.getNestedElement(data, 'facility_code');
		date = fn.getNestedElement(data, 'upload_date');
		Logger.v('date', date)
		date_string = DateTime.toString(date);
		values = {
			'state_name': state_name,
			'state_code': state_code,
			'facility_name': facility_name,
			'facility_code': facility_code,
			'state_updated_at': date_string,
			'facility_updated_at': date_string,
			'date': date_string,
		};
		dbManager.addBulkInsert(collection_name, values, batch=True);
		unique_facility.append(state_facility_code);
	dbManager.executeBulkOperations(collection_name);

def generateIndex():
	global collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	db[collection_name].create_index([('state_updated_at', -1), ('facility_updated_at', -1)]);
	db[collection_name].create_index([('state_name', 1), ('state_code', 1), ('facility_name', 1), ('facility_code', 1)]);