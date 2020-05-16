from lib import Logger;
from lib import fn;
from lib import SharedMemoryManager;
from lib import DebugManager;
from lib import Params;
from lib import basic;
from lib import Mail;
from lib import DateTime;
from Report import File;
from . import Queue;
from . import Main as Crawl;
import traceback;
import time;
import requests;
import os;
import json;
import pytz;
import random;
import pandas as pd;
import copy;
from datetime import datetime;

project_root_folder = fn.getRootPath();
collection_name = 'crawl_queue';
collection_name_setting = 'crawl_queue_setting';
# filter_page_type = ['ig'];
filter_page_type = fn.getNestedElement(fn.config,'CRAWL_QUEUE_TYPE','budget,procurement').split(',');
storage_folder = '/tests/';
crawl_folder = '{0}/crawled_data'.format(project_root_folder);
test_folder = '{0}/tests'.format(project_root_folder);
msia_tz = pytz.timezone('Asia/Kuala_Lumpur');
pass_month_quantity = 1 * 12;
collection_unique_keys = {
	'state': ['state_seq_no', 'state_code', 'state_name'],
	'ptj': ['ptj_seq_no', 'ptj_code', 'ptj_name'],
	'facility': ['facility_seq_no', 'facility_code', 'facility_name'],
	'facility_type': ['facility_seq_no', 'facility_code', 'facility_name', 'facility_type'],
	'budget': ['facility_seq_no', 'ptj_seq_no', 'budget_type_seq_no', 'object_seq_no', 'item_group_seq_no', 'financial_year'],
	'procurement': ['facility_seq_no', 'ptj_seq_no', 'txn_year', 'txn_month', 'item_type_seq_no', 'drug_seq_no', 'item_packing_seq_no'],
};
api_links = {
	'state': 'http://10.1.1.103:8080/bizintservice/masters/state', 
	'ptj': 'http://10.1.1.103:8080/bizintservice/masters/ptjcodes', 
	'facility': 'http://10.1.1.103:8080/bizintservice/masters/facility', 
	'facility_type': 'http://10.1.1.103:8080/bizintservice/masters/factype',
	'test_purpose': 'https://jsonplaceholder.typicode.com/todos/1',
	# 'budget': 'http://10.1.1.103:8080/bizintservice/bireports/budgetsummary?stateCode=15&financialYear=2018',
	# 'budget_facility': 'http://10.1.1.103:8080/bizintservice/bireports/budgetsummary?stateCode=15&financialYear=2018&facilityCode=21-15010004',
	# 'procurement': 'http://10.1.1.103:8080/bizintservice/bireports/procurementdetails?stateCode=15&startdate=20190301&enddate=20190430',
	# 'procurement_facility': 'http://10.1.1.103:8080/bizintservice/bireports/procurementdetails?stateCode=15&startdate=20190301&enddate=20190430&facilityCode=21-15010004',
	# 'budget': 'http://10.1.1.103:8080/bizintservice/bireports/budgetsummary?stateCode={}&financialYear={}',
	# 'budget_facility': 'http://10.1.1.103:8080/bizintservice/bireports/budgetsummary?stateCode={}&financialYear={}&facilityCode={}',
	# 'procurement': 'http://10.1.1.103:8080/bizintservice/bireports/procurementdetails?stateCode={}&startdate={}&enddate={}',
	# 'procurement_facility': 'http://10.1.1.103:8080/bizintservice/bireports/procurementdetails?stateCode={}&startdate={}&enddate={}&facilityCode={}',
	'budget': 'http://10.1.1.103:8080/bizintservice/bireports/budgetsummary?stateCode={}&financialYear={}&facilityCode={}',
	'procurement': 'http://10.1.1.103:8080/bizintservice/bireports/procurementdetails?stateCode={}&startdate={}&enddate={}&facilityCode={}',
};
api_files = {
	'state': '{0}/sample_data/csv/state/2020-02-28.json'.format(project_root_folder), 
	'ptj': '{0}/sample_data/csv/ptj/2020-02-28.json'.format(project_root_folder), 
	'facility': '{0}/sample_data/csv/facility/2020-02-28.json'.format(project_root_folder), 
	'facility_type': '{0}/sample_data/csv/facility_type/2020-02-28.json'.format(project_root_folder),
	'budget': '{0}/sample_data/csv/budget/2020-02-28.json'.format(project_root_folder),
	'budget_facility': '{0}/sample_data/csv/budget_facility/2020-02-28.json'.format(project_root_folder),
	'procurement': '{0}/sample_data/csv/procurement/2020-02-28.json'.format(project_root_folder),
	'procurement_facility': '{0}/sample_data/csv/procurement_facility/2020-02-28.json'.format(project_root_folder),
};

global_check_data = {};
global_text = {};
global_start_count = 0;

def run(params):
	Mail.send('{0} Crawl - Store starts'.format(DateTime.getReadableDate(DateTime.now())), 'Start at: {0}'.format(DateTime.now(tzinfo=msia_tz)));
	recordTime(key='create_schedule_start_time');
	Debug = DebugManager.DebugManager();
	Debug.start();
	start_crawl = fn.getNestedElement(params, 'schedule_params.start_crawl', False);
	check_empty = fn.getNestedElement(params, 'schedule_params.check_empty', False);
	Logger.v('Creating schedule:');
	updateDropdownOptions(params=params);
	crawl_params = generateCrawlParam(params);
	Debug.trace('Generate Crawl Params');
	createSchedules({'pages': crawl_params});
	Debug.trace('Create Schedule');
	if start_crawl:
		Crawl.start(params);
		Debug.trace('crawling');
	recordTime(key='create_schedule_end_time');
	Debug.show('Run')

def updateDropdownOptions(params):
	option_keys = fn.getNestedElement(params, 'keys.option', ['state']);
	today = fn.getNestedElement(params, 'schedule_params.today', DateTime.toString(DateTime.now(tzinfo=msia_tz)));
	data = {};
	crawled_data = {};
	# crawl from API URL (get options from API)
	# for key in keys:
	# 	url = api_links[key];
	# 	# url = generateUrl(api_links[key]);		
	# 	response = requests.get(url);
	# 	json_response = json.loads(response.text);
	# 	Logger.v('json_response', json_response);
	# 	crawled_data[key] = json_response;
	# 	Logger.v('Crawled', url);
	# Logger.v('Done crawling.');
	# save(data);

	# read from file
	for key in option_keys:
		filename = api_files[key];
		crawled_data[key] = File.readJson(filename);

	# convert key to snakecase, value to lower
	for key in crawled_data:
		if key not in data:
			data[key] = [];
		for idx in range(0, len(crawled_data[key])):
			row = crawled_data[key][idx];
			obj_ = {};
			for row_key in row:
				row_value = row[row_key];
				new_key = fn.camelToSnakecase(str=row_key);
				if type(row_value) == str:
					new_value = row_value.lower();
				elif row_value is None:
					new_value = 'null';
				else:
					new_value = row_value;
				obj_[new_key] = new_value;

			data[key].append(obj_);

	for key in data:
		folder_path = '/'.join([crawl_folder, key]);
		if not os.path.exists(folder_path):
			os.makedirs(folder_path);
		filename = '{0}/{1}'.format(folder_path, today);
		Logger.v('Saving', filename);
		fn.writeJSONFile(filename='{0}.json'.format(filename), data=data[key]);

	for key in option_keys:
		directory = '/'.join([crawl_folder, key]);
		raw = File.readLatestFile(directory=directory);
		refresh_collection = refreshIsRequired(data=raw, collection_name=key);
		if refresh_collection:
			refreshCollection(data=raw, collection_name=key);
			Logger.v('refreshed', key);

def generateCrawlParam(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	global pass_month_quantity;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	crawl_params = {};
	limit_for_test = 10;
	report_keys = fn.getNestedElement(params, 'keys.report', ['budget', 'procurement']);
	interval = fn.getNestedElement(params, 'interval', 1);
	filter_facility_code = fn.getNestedElement(params, 'filter.facility_code', True);
	check_empty = fn.getNestedElement(params, 'schedule_params.check_empty', False);
	today = fn.getNestedElement(params, 'schedule_params.today', DateTime.toString(DateTime.now(tzinfo=msia_tz)));
	# Logger.v('filter_facility_code', filter_facility_code);
	if check_empty:
		# past_dates = DateTime.getPastDate(count=pass_month_quantity, duration=interval);
		past_dates = DateTime.getPastDate(count=pass_month_quantity, duration=interval, end=DateTime.convertDateTimeFromString(today));
		# Logger.v('past_dates', past_dates);
		# exit();
	else:
		past_dates = DateTime.getPastDate(count=pass_month_quantity, duration=interval);
		
	# Logger.v('past_dates', past_dates);
	state_codes = retrieveOption(collection_name='state', show_keys=['state_code'], hide_keys=['_id']);
	state_code = extractListByKey(data=state_codes, key='state_code');
	facility_codes = retrieveOption(collection_name='facility', show_keys=['facility_code'], hide_keys=['_id']);
	facility_code = extractListByKey(data=facility_codes, key='facility_code');
	for key in report_keys:
		# Logger.v('collection', key, past_dates[0]);
		Debug.trace();
		if key not in crawl_params:
			crawl_params[key] = [];
		mongo_data = list(db[key].find({}, {}));
		
		if len(mongo_data) == 0:
			dates = past_dates[0][:];
		else:
			dates = past_dates[0][:1];

		year = extractYear(data=dates);
		# Logger.v('year', year);
		# Logger.v('filter_facility_code', filter_facility_code);
		if key == 'budget':
			if not filter_facility_code:
				iteration = 0;
				total = len(year)*len(state_code);
				# fn.printProgressBar(iteration=iteration, total=total);
				for y in year:
					for sc in state_code:
						obj_ = {
							'financial_year': y,
							'state_code': sc,
							'page_type': key,
							'upid': '_'.join([sc, y]),
							'url': api_links[key].format(sc, y, ''),
							'start_date': today,
							'end_date': today,
						};
						if obj_ not in crawl_params[key]:
							crawl_params[key].append(obj_);
							# Logger.v('len(crawl_param])', len(crawl_params[key]));
						iteration += 1;
						# fn.printProgressBar(iteration=iteration, total=total);
			else:
				iteration = 0;
				total = len(year)*len(state_code)*len(facility_code[:limit_for_test]);
				# fn.printProgressBar(iteration=iteration, total=total);
				for y in year:
					for sc in state_code:
						for fc in facility_code[:limit_for_test]:
							obj_ = {
								'financial_year': y,
								'state_code': sc,
								'page_type': key,
								'upid': '_'.join([sc, y, fc]),
								'facility_code': fc,
								'url': api_links[key].format(sc, y, fc),
								'start_date': today,
								'end_date': today,
							};
							if obj_ not in crawl_params[key]:
								crawl_params[key].append(obj_);
								# Logger.v('len(crawl_param])', len(crawl_params[key]));
							iteration += 1;
							# fn.printProgressBar(iteration=iteration, total=total);

		elif key == 'procurement':
			if not filter_facility_code:
				for past_duration in dates:
					start_date = DateTime.toString(DateTime.getDaysAgo(days_to_crawl=-1, datefrom=past_duration[0]));
					end_date = DateTime.toString(DateTime.getDaysAgo(days_to_crawl=1, datefrom=past_duration[1]));
					for sc in state_code:
						obj_ = {
							'state_code': sc,
							'start_date': start_date,
							'end_date': end_date,
							'page_type': key,
							'upid': '_'.join([sc, start_date, end_date]),
							'url': api_links[key].format(sc, start_date.replace('-', ''), end_date.replace('-', ''), ''),
						};

						if obj_ not in crawl_params[key]:
							crawl_params[key].append(obj_);
							# Logger.v('len(crawl_param])', len(crawl_params[key]));
			else:
				for past_duration in dates:
					start_date = DateTime.toString(DateTime.getDaysAgo(days_to_crawl=-1, datefrom=past_duration[0]));
					end_date = DateTime.toString(DateTime.getDaysAgo(days_to_crawl=1, datefrom=past_duration[1]));
					for sc in state_code:
						for fc in facility_code[:limit_for_test]:
							obj_ = {
								'state_code': sc,
								'start_date': start_date,
								'end_date': end_date,
								'page_type': key,
								'facility_code': fc,
								'upid': '_'.join([sc, start_date, end_date, fc]),
								'url': api_links[key].format(sc, start_date.replace('-', ''), end_date.replace('-', ''), fc)
							};
							if obj_ not in crawl_params[key]:
								crawl_params[key].append(obj_);
								# Logger.v('len(crawl_param])', len(crawl_params[key]));


	for c in crawl_params:
		# Logger.v('crawl_params', c, len(crawl_params[c]));
		fn.writeExcelFile(filename='{0}/{1}'.format(test_folder, c), data=crawl_params[c]);
	Logger.v('crawl_params', len(crawl_params));
	Debug.show('Generate Crawl Params');
	return crawl_params;

def extractListByKey(data, key):
	result = [];
	for idx in range(0, len(data)):
		result.append(data[idx][key]);
	return result;

def extractYear(data):
	year = [];
	for past_duration in data:
		# Logger.v('len(past_duration)', len(past_duration))
		for idx in range(len(past_duration)-1, -1, -1):
			pd = past_duration[idx];
			# Logger.v('pd', pd);
			y = DateTime.getDateCategoryName(date=pd, element='year', offset=8);
			if y not in year:
				year.append(y);
	return year;

def retrieveOption(collection_name='state', show_keys=[], hide_keys=[]):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	return_keys = {};
	for key in show_keys:
		return_keys.update({key: 1});
	for key in hide_keys:
		return_keys.update({key: 0});

	# Logger.v('return_keys', return_keys);
	if return_keys:
		result = list(db[collection_name].find({}, return_keys));
	else:
		result = list(db[collection_name].find({}));

	return result;

def generateUniqueValue(data, collection_name, mode='dict'):
	result = [];
	for d in data:
		key = fn.camelToSnakecase(str=d);
		if key in collection_unique_keys[collection_name]:
			v = str(data[d]).strip();
			result.append(v.lower().replace(' ', '_'));
	return result;

def generateKeyValue(data):
	result = {};
	for d in data:
		key = fn.camelToSnakecase(str=d);
		k = str(key).strip();
		v = str(data[d]).strip();
		result.update({k: v});
	return result;

def refreshCollection(data, collection_name):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	db[collection_name].delete_many({});
	for row in data:
		unique_value = generateUniqueValue(data=row, collection_name=collection_name);
		obj_ = generateKeyValue(data=row);
		obj_.update({'unique_value': '_'.join(unique_value)});
		# Logger.v('obj_', obj_);
		dbManager.addBulkInsert(collection_name, obj_, batch=True);
	dbManager.executeBulkOperations(None);

def refreshIsRequired(data, collection_name):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	refresh_collection = False;
	mongo_data = list(db[collection_name].find({}));
	# Logger.v('mongo_data', mongo_data);
	unique_values = [];
	for row in data:
		obj_ = {};
		unique_value = generateUniqueValue(data=row, collection_name=collection_name);
		unique_values.append('_'.join(unique_value));
	matched_row = db[collection_name].find({'unique_value': {'$in': unique_values}});
	matched_result = list(matched_row);
	# Logger.v('matched_result', matched_result)
	if not len(matched_result) == len(mongo_data) or len(mongo_data) == 0: # if there is difference between mongodb and raw
		Logger.v('matched_result len', len(matched_result))
		Logger.v('mongo_data len', len(mongo_data))
		refresh_collection = True;
		return refresh_collection;
	return refresh_collection

# def generateUrl(url):
# 	subroute = url.split('/')[0];
# 	subroute_params = {
# 		'procurementdetails': '?stateCode={}&startdate={}&enddate={}&facilityCode={}',
# 		'budgetsummary': '?stateCode={}&financialYear={}&facilityCode={}',
# 	}
# 	base_url = '{}?api_key={}'.format(url, ''); #setup base url
# 	return base_url + '&startDate={}&endDate={}&pageSize={}&storeId={}&pageNumber={}';

# def convertPageList(page_list, insights=False, exclude={}):
# 	if exclude and not insights:
# 		exclude = list(exclude.keys());
# 	Logger.v('List to exclude:%s'% exclude);
# 	pages = {};
# 	for p in page_list:
# 		upid = fn.getNestedElement(p, 'upid');

# 		if 'token' in p:
# 			del p['token'];
# 		if exclude:
# 			if insights:
# 				if max([len(x) for x in fn.getNestedElement(exclude, upid, [])]) > 0:
# 					continue;
# 			elif upid in exclude:
# 				continue;
# 		pages[upid] = p;
# 	return pages;

# def createScheduleByPlatform(platform, exclude_list, upid=None, mode=''):
# 	page_list = Page.getAllDetail({ 'page_type': platform }, mode=mode);
# 	pages = convertPageList(page_list,insights=(mode =='page_access_token') ,exclude=exclude_list);
# 	Logger.v('Crawl.createScheduleByPlatform: %s: Length: %s. Mode: %s'%(platform, len(pages), mode));
# 	return pages;

def createSchedules(args={}): #upid, page_type
	global filter_page_type;
	Debug = DebugManager.DebugManager();
	Debug.start();
	dbManager = SharedMemoryManager.getInstance();
	# crawl_duration = fn.getNestedElement(fn.config,'CRAWL_DURATION', 12);
	incomplete_task, incomplete_task_count = checkRemaining();
	new_queue_count = 0;
	# Logger.v(incomplete_task_count, incomplete_task, filter_page_type);
	extra_params = { 'crawl_comment': fn.getNestedElement(args, 'crawl_comment', None) };
	extra_params = {k: v for k, v in extra_params.items() if v is not None}

	for platform in filter_page_type:
		if args and not platform in fn.getNestedElement(args, 'page_type', platform).split(','):
			Logger.v('Skip Platform:%s'%(platform));
			continue; # skip when page_type appear and not same
		pages = fn.getNestedElement(args, 'pages.{0}'.format(platform), []);
		Logger.v('platform', platform)
		# Logger.v('page', args['pages']['budget']);
		for page in pages: #Create queue for each 
			# Logger.v('page', page);
			Queue.create(page, extra_params=extra_params, priority=fn.getNestedElement(args, 'priority', 'daily'), batch=True);
			new_queue_count +=1;
			Logger.v('new_queue_count', new_queue_count);
		# Debug.trace();

	Logger.v('Incomplete:%s, New Queue: %s'%(incomplete_task_count, new_queue_count));
	if incomplete_task_count > (new_queue_count*int(fn.config['DEBUG_CRAWL_WARNING'])/100) or incomplete_task_count > int(fn.config['DEBUG_CRAWL_WARNING']):
		# Mail.send('[%s]Incomplete Crawl [%s], Current Schedule: [%s]'%(DateTime.getReadableDate(DateTime.now()), 
		# 	incomplete_task_count, new_queue_count),
		# 		 fn.dumps(incomplete_task, encode=False)
		# );
		pass;

	result = { 'pending_count':new_queue_count, 'incomplete_count':incomplete_task_count };
	dbManager.executeBulkOperations(None);
	# Debug.show('Create Schedule');
	return Params.generate(True, result);

def checkRemaining():
	schedules = Queue.getPending();
	# Logger.v('checkRemaining', schedules)
	incomplete_task = {};
	for schedule in schedules:
		upid = schedule['upid'];
		page_type = schedule['page_type'];
		if not page_type in incomplete_task:
			incomplete_task[page_type] = {};
		# Logger.v('incomplete_task[page_type]', incomplete_task[page_type]);
		# Logger.v('incomplete_task[page_type]', type(incomplete_task[page_type]), 'type upid', type(upid));
		if not upid in incomplete_task[page_type]:
			incomplete_task[page_type][upid] = [];
		# Logger.v('incomplete_task[page_type][upid]', incomplete_task[page_type][upid]);
		incomplete_task[page_type][upid].append(fn.getNestedElement(schedule, 'page_access_token', ''));
	count = sum([len(incomplete_task[x].keys()) for x in incomplete_task]);
	return (incomplete_task, count);

def recordTime(key):
	global global_text;
	new_key = key.replace('end', 'start');
	if new_key not in global_text:
		global_text[new_key] = DateTime.now(tzinfo=msia_tz);

	if new_key in global_text and not key == new_key:
		global_text[key] = DateTime.now(tzinfo=msia_tz);
