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
from . import Schedule;
from . import Recrawl;
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
from bs4 import BeautifulSoup;

project_root_folder = fn.getRootPath();
collection_name = 'crawl_queue';
collection_name_setting = 'crawl_queue_setting';
# filter_page_type = ['ig'];
filter_page_type = fn.getNestedElement(fn.config,'CRAWL_QUEUE_TYPE','budget,procurement').split(',');
storage_folder = '/tests/';
crawl_folder = '{0}/crawled_data'.format(project_root_folder);
test_folder = '{0}/tests'.format(project_root_folder);
msia_tz = pytz.timezone('Asia/Kuala_Lumpur');
collection_unique_keys = {
	'state': ['state_seq_no', 'state_code', 'state_name'],
	'ptj': ['ptj_seq_no', 'ptj_code', 'ptj_name'],
	'facility': ['facility_seq_no', 'facility_code', 'facility_name'],
	'facility_type': ['facility_seq_no', 'facility_code', 'facility_name', 'facility_type'],
	'budget': ['facility_seq_no', 'ptj_seq_no', 'budget_type_seq_no', 'object_seq_no', 'item_group_seq_no', 'financial_year'],
	'procurement': ['facility_seq_no', 'ptj_seq_no', 'txn_year', 'txn_month', 'item_type_seq_no', 'drug_seq_no', 'item_packing_seq_no'],
};
report_keys_to_update = {
	'state': ['state_seq_no', 'state_code', 'state_name'],
	'ptj': ['ptj_seq_no', 'ptj_code', 'ptj_name'],
	'facility': ['facility_seq_no', 'facility_code', 'facility_name'],
	'facility_type': ['facility_seq_no', 'facility_code', 'facility_name', 'facility_type'],
	'budget': ['object_seq_no', 'object_code', 'object_desc', 'item_group_seq_no', 'item_group_code', 'first_allocation', 'additional_allocation', 'pending_amount', 'utilized_amount', 'liablity_amount', 'trans_in_amount', 'trans_out_amount', 'deduction_amount', 'current_actual_amount', 'ptj_seq_no', 'ptj_code', 'ptj_name'],
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
	'state': '{0}/crawled_data/csv/state/2020-02-28.json'.format(project_root_folder), 
	'ptj': '{0}/crawled_data/csv/ptj/2020-02-28.json'.format(project_root_folder), 
	'facility': '{0}/crawled_data/csv/facility/2020-02-28.json'.format(project_root_folder), 
	'facility_type': '{0}/crawled_data/csv/facility_type/2020-02-28.json'.format(project_root_folder),
	'budget': '{0}/crawled_data/csv/budget/2020-02-28.json'.format(project_root_folder),
	'budget_facility': '{0}/crawled_data/csv/budget_facility/2020-02-28.json'.format(project_root_folder),
	'procurement': '{0}/crawled_data/csv/procurement/2020-02-28.json'.format(project_root_folder),
	'procurement_facility': '{0}/crawled_data/csv/procurement_facility/2020-02-28.json'.format(project_root_folder),
};

global_check_data = {};
global_text = {};
global_start_count = 0;


def sourceBursaMalaysia(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');
	url = 'https://www.bursamalaysia.com/market_information/equities_prices';
	page = requests.get(url);
	soup = BeautifulSoup(page.text, 'html.parser');
	total_page = int(soup.find('li', id='total_page').get('data-val'));
	limit_page = None;
	Logger.v('total pages', total_page);
	# print(soup.find_all('h2'));
	date = '';
	time = '';
	for div in soup.find_all('div'):	
		if div.get('class') == ['col', 'bg-white', 'mr-1', 'mb-0', 'h5', 'bold', 'p-2'] and date == '':
			# print(div.get_text().strip());
			date = div.get_text().strip();
		if div.get('class') == ['col', 'bg-white', 'h5', 'mb-0', 'bold', 'p-2'] and time == '':
			# print(div.get_text().strip());
			time = div.get_text().strip();
	Logger.v('date', date, 'time', time);
	result = [];
	Debug.trace('get detail');
	for page in range(1, total_page + 1)[:limit_page]:
		fn.printProgressBar(iteration=page, total=total_page + 1, prefix = 'Page:{0}'.format(page));
		url_with_page = 'https://www.bursamalaysia.com/market_information/equities_prices?page={0}'.format(page);
		page = requests.get(url_with_page);
		soup = BeautifulSoup(page.text, 'html.parser');
		tables = soup.find_all('table');
		for table in tables: 
			if table.get('class') == ['table', 'datatable-striped', 'text-center', 'equity_prices_table', 'datatable-with-sneak-peek', 'js-anchor-price-table']: # only take first table (price)
				headers = getTableHeader(data=table);
				headers += ['date', 'time'];
				content = getTableContent(data=table);
				for row in content:
					row += [date, time];
					result.append(dict(zip(headers, row)));
				# Logger.v('headers', headers)

	# Logger.v('result', len(result), result);
	Debug.trace('crawl price');
	save_dir = 'crawled_data/price';
	filename = '{0}_{1}_price.csv'.format(date, time);
	fn.ensureDirectory(save_dir);
	save_file = '{0}/{1}'.format(save_dir, filename);
	df = pd.DataFrame(result);
	df.to_csv(save_file, index=False);
	Logger.v('saved {0}'.format(save_file));
	Debug.trace('save price');

	saveStockList(data=result);
	Debug.trace('save stock list');
	Debug.end();
	Debug.show('sourceBursaMalaysia');
	return True;

def saveStockList(data):
	df = pd.DataFrame(data);
	# df.info();
	crawled_stocks = df[['code', 'name']];
	# crawled_stocks.info();

	save_dir = 'reference/stock_info';
	fn.ensureDirectory(save_dir);
	save_file = '{0}/stock_list.csv'.format(save_dir);

	try:
		exist_stocks = pd.read_csv(save_file);
	except Exception as ex:
		Logger.v('ex', ex);
		exist_stocks = pd.DataFrame(columns=['code', 'name']);

	# exist_stocks.info();
	merged_stocks = pd.concat([exist_stocks, crawled_stocks]).drop_duplicates();
	# merged_stocks.info();
	merged_stocks.to_csv(save_file, index=False);
	Logger.v('saved {0}'.format(save_file));

def sourceKlsescreener(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');

	stock_list = pd.read_csv('reference/stock_info/stock_list.csv');
	stock_codes = stock_list['code'].unique().tolist();
	Debug.trace('read stock list');
	summary = [];
	error = [];
	for code in stock_codes:
		result = {};
		url = 'https://www.klsescreener.com/v2/stocks/view/{0}'.format(code);
		Logger.v('crawling:', url);

		page = '';
		error_count = 0;
		while page == '':
			try:
				page = requests.get(url);
				error_count = 0;
				break
			except:
				if error_count > 3:
					page = 'None';
					error_count = 0;
					break;
				print("Connection refused by the server..")
				print("Let me sleep for 5 seconds")
				print("ZZzzzz...")
				time.sleep(5)
				print("Was a nice sleep, now let me continue...")
				error_count += 1;
				continue

		if not page == '':
			time.sleep(3);
			soup = BeautifulSoup(page.text, 'html.parser');
			# print(soup.prettify());
			# print(soup.find_all('div', id='quarter_reports'));
			# Logger.v('Quarter Report');
			quarter_reports_div = soup.find_all('div', id='quarter_reports');
			result['quarter_reports'] = getQuarterData(data=quarter_reports_div);

			# Logger.v('Annual Report'); # th in tbody
			annual_report_div = soup.find_all('div', id='annual');
			result['annual'] = getAnnualData(data=annual_report_div);

			# Logger.v('Dividend');
			dividends_div = soup.find_all('div', id='dividends');
			result['dividends'] = getQuarterData(data=dividends_div);

			# Logger.v('Capital Changes');
			capital_changes_div = soup.find_all('div', id='capital_changes');
			result['capital_changes'] = getQuarterData(data=capital_changes_div);

			# Logger.v('Warrants'); # every row got new tbody
			warrants_div = soup.find_all('div', id='warrants');
			result['warrants'] = getQuarterData(data=warrants_div);

			# Logger.v('Shareholding Changes'); # every row got new tbody
			shareholding_changes_div = soup.find_all('div', id='shareholding_changes');
			result['shareholding_changes'] = getQuarterData(data=shareholding_changes_div);
			Debug.trace('crawl report');

			save_dir = 'crawled_data/reports';
			fn.ensureDirectory(save_dir);
			save_file = '{0}/{1}.json'.format(save_dir, code);
			fn.writeJSONFile(save_file, result);

			summary.append({
				'code': code,
				'quarter_reports': len(result['quarter_reports']),
				'annual': len(result['annual']),
				'dividends': len(result['dividends']),
				'capital_changes': len(result['capital_changes']),
				'warrants': len(result['warrants']),
				'shareholding_changes': len(result['shareholding_changes']),
			});
			Debug.trace('save report');
		elif page == 'None':
			error.append({'code': code});
	# Logger.v('summary', summary);
	summary_file = '{0}/summary/klsescreener_report'.format(save_dir);
	fn.writeExcelFile(filename=summary_file, data=summary);
	Debug.trace('save summary');
	error_file = '{0}/error/klsescreener_report'.format(save_dir);
	fn.writeExcelFile(filename=error_file, data=error);
	Debug.trace('save error');
	Debug.end();
	Debug.show('sourceKlsescreener');
	return True;
	

def getAnnualData(data):
	result = [];
	for d in data:
		headers = getTableHeader2(data=d);
		content = getTableContent2(data=d);
		for row in content:
			result.append(dict(zip(headers, row)));
		# print(result);
		# print(len(result));
	return result;

def getQuarterData(data):
	result = [];
	for d in data:
		headers = getTableHeader(data=d);
		content = getTableContent(data=d);
		for row in content:
			result.append(dict(zip(headers, row)));
		# print(result);
		# print(len(result));
	return result;

def getTableHeader2(data):
	
	thead = data.table.find_all('th');
	result = [];
	for th in thead:
		# print(th.get_text());
		result.append(fn.convertToSnakecase(th.get_text()));
	# print(result);

	# Logger.v('main_result', main_result);
	# exit();
	return result;

def getTableHeader(data):
	
	thead = data.thead.find_all('th');
	result = [];
	for th in thead:
		# print(th.get_text());
		result.append(fn.convertToSnakecase(th.get_text()));
	# print(result);
	
	# Logger.v('main_result', main_result);
	# exit();
	return result;

def getTableContent2(data):
	result = [];
	tbody = data.table.find_all('tr');
	for tr in tbody[1:]:
		# print(tr)
		row = [];
		for cell in tr.find_all('td'):
			# print(cell.get_text());
			row.append(cell.get_text().strip().lower());
		result.append(row);
		# print(result);
		# exit();
	return result;

def getTableContent(data):
	tbody = data.find_all('tbody');
	# tbody_count = len(tbody);
	# if tbody_count > 1:
	result = [];
	for body in tbody:
		trs = body.find_all('tr');
		for tr in trs:
			# print(tr)
			row = [];
			for cell in tr.find_all('td'):
				# print(cell.get_text());
				row.append(cell.get_text().strip().lower());
			result.append(row);
			# print(result);
			# exit();
	return result;






# def log(case, args):
# 	ip = basic.getCurrentIP();
# 	if case == 'crawl-start':
# 		# if fn.getNestedElement(args, 'priority', '') == 'new' :
# 		# 	Mail.send('[%s]New Profile Start Crawl %s@%s'%( DateTime.getReadableDate(DateTime.now()), fn.getNestedElement(args, 'name'), ip ), '%s'%(args));
# 		Logger.v('args', case);
# 	elif case == 'crawl-end':
# 		# if fn.getNestedElement(args, 'priority', '') == 'new' :
# 		# 	Mail.send('[%s]New Profile End Crawl %s@%s'%( DateTime.getReadableDate(DateTime.now()), fn.getNestedElement(args, 'name'), ip ), '');
# 		Logger.v('args', case);

# def run(params):
# 	Debug = DebugManager.DebugManager();
# 	Debug.start();
# 	process = fn.getNestedElement(params, 'process', ['schedule', 'crawl', 'update']);
# 	start_crawl = fn.getNestedElement(params, 'start_crawl', False);
# 	Debug.trace('start');
# 	for p in process:
# 		if p == 'schedule':
# 			fn.writeTestFile(filename='cron_schedule_start_{0}'.format(DateTime.now()), data='schedule start');
# 			Schedule.run(params);
# 			Debug.trace('generate schedule');
# 		elif p == 'crawl' and not start_crawl:
# 			fn.writeTestFile(filename='cron_start_start_{0}'.format(DateTime.now()), data='start start');
# 			start(params);
# 			Debug.trace('start crawl');
# 		elif p == 'update':
# 			fn.writeTestFile(filename='cron_update_start_{0}'.format(DateTime.now()), data='update start');
# 			updateReportData(params);
# 			Debug.trace('update mongo');

# 	Debug.end();
# 	Debug.show('Main.run');

# def start(params):
# 	global global_start_count;
# 	recordTime(key='crawl_start_time');
# 	global_start_count += 1;
# 	Logger.v('global_start_count', global_start_count);
# 	Debug = DebugManager.DebugManager();
# 	Debug.start();
# 	dbManager = SharedMemoryManager.getInstance();
# 	check_empty = fn.getNestedElement(params, 'schedule_params.check_empty', False);
# 	recrawl_qty = 2;
# 	incomplete_task, incomplete_task_count = Schedule.checkRemaining();

# 	# if int(fn.config['DEBUG_CRAWL_CRON'])  == 1:
# 		# incomplete_task = checkRemainingSchedule()['count'];
# 		# Mail.send('[%s]Crawl Cron, %s incomplete task.'%( DateTime.getReadableDate(DateTime.now()), len(incomplete_task) ), '');
# 	ip = basic.getCurrentIP();
# 	# Logger.v('ip', ip);
# 	# Logger.v('Queue.getRunning(ip)', Queue.getRunning(ip));
# 	# Logger.v('max crawl', int(fn.getNestedElement(fn.config, 'MAXIMUM_CRAWL_PER_NODE', '1')));
# 	while (Queue.getRunning(ip)< int(fn.getNestedElement(fn.config, 'MAXIMUM_CRAWL_PER_NODE', '1'))):
# 		nextSchedule = Queue.getNext(ip);
# 		# Logger.v('nextSchedule', nextSchedule)
# 		if nextSchedule:
# 			page_type = nextSchedule['page_type'];
# 			Logger.v('Crawling :', nextSchedule);
# 			try:
# 				# log('crawl-start', nextSchedule);
# 				qid = nextSchedule['_id'];
# 				Queue.start(qid);
# 				# Logger.v('qid', qid)
# 				crawl(nextSchedule);
# 				Queue.end(qid);
# 				# log('crawl-end', nextSchedule);
# 				Debug.trace();
# 			except Exception as ex:
# 				traceback.print_exc();
# 				# Logger.v('except')
# 				Queue.create(nextSchedule, priority='re-crawl');
# 		else:
# 			break;
# 	dbManager.executeBulkOperations(None);

# 	incomplete_task, incomplete_task_count = Schedule.checkRemaining();

# 	if check_empty:
# 		params['schedule_params']['check_empty'] = False;
# 		params['schedule_params']['start_crawl'] = True;
# 		for idx in range(0, recrawl_qty):
# 			recordTime(key='recrawl_{0}_start_time'.format(idx));
# 			data = Recrawl.checkEmpty(params);
# 			Recrawl.run(params=params, data=data);
# 			recordTime(key='recrawl_{0}_end_time'.format(idx));

# 	recordTime(key='crawl_end_time');

# 	Debug.end();
# 	Debug.show('PhIS Crawler.');
# 	return True;

# def crawl(schedule):
# 	page_type = fn.getNestedElement(schedule, 'page_type');
# 	url = fn.getNestedElement(schedule, 'url');
# 	financial_year = fn.getNestedElement(schedule, 'financial_year', '');
# 	state_code = fn.getNestedElement(schedule, 'state_code', '');
# 	facility_code = fn.getNestedElement(schedule, 'facility_code', 'all_facility');
# 	start_date = fn.getNestedElement(schedule, 'start_date', '');
# 	end_date = fn.getNestedElement(schedule, 'end_date', '');
# 	# Logger.v('folder_path_map', page_type);

# 	folder_path_map = {
# 		'budget': '/'.join([crawl_folder, page_type, 'year_{0}'.format(financial_year), 'state_{0}'.format(state_code), facility_code]),
# 		'procurement': '/'.join([crawl_folder, page_type, 'year_{0}'.format(start_date[:4]), 'state_{0}'.format(state_code), '_'.join([start_date[:-3], end_date[:-3]]), facility_code]),
# 	};
# 	folder_path = folder_path_map[page_type];
# 	data = [];
# 	# generate demo data
# 	crawled_data = generateDemoData(page_type, detail=schedule);

# 	# crawl real data from api
# 	# response = requests.get(url);
# 	# crawled_data = json.loads(response.text);
# 	# Logger.v('crawling url:', url);

# 	# convert key to snakecase, value to lower
# 	for idx in range(0, len(crawled_data)):
# 		row = crawled_data[idx];
# 		obj_ = {};
# 		for row_key in row:
# 			row_value = row[row_key];
# 			new_key = fn.camelToSnakecase(str=row_key);
# 			if type(row_value) == str:
# 				new_value = row_value.lower();
# 			elif row_value is None:
# 				new_value = 'null';
# 			else:
# 				new_value = row_value;
# 			obj_[new_key] = new_value;

# 		data.append(obj_);

# 	if not os.path.exists(folder_path):
# 		os.makedirs(folder_path);
# 	if data:
# 		filename = '{0}/{1}'.format(folder_path, end_date);
# 		Logger.v('Saving', filename);
# 		fn.writeJSONFile(filename='{0}.json'.format(filename), data=data);

# def generateDemoData(page_type, detail):
# 	Logger.v('Generating Demo Data...')
# 	# Logger.v('page_type', page_type);
# 	# raw = File.readJson(filename=api_files[page_type]);
# 	# Logger.v('raw', len(raw));
# 	financial_year = fn.getNestedElement(detail, 'financial_year');
# 	state_code = fn.getNestedElement(detail, 'state_code');
# 	facility_code = fn.getNestedElement(detail, 'facility_code');
# 	start_date = fn.getNestedElement(detail, 'start_date');
# 	end_date = fn.getNestedElement(detail, 'end_date');

# 	states = retrieveOption(collection_name='state', show_keys=[], hide_keys=['_id']);
# 	facility_types = retrieveOption(collection_name='facility_type', show_keys=[], hide_keys=['_id']);
# 	ptjs = retrieveOption(collection_name='ptj', show_keys=[], hide_keys=['_id']);
# 	result = [];
# 	for idx in range(0, 1000):
# 		# row = raw[idx];
# 		row = {};
# 		if page_type == 'budget':
# 			budget_type = random.choice(budget_type_table);
# 			object_ = random.choice(object_table);
# 			item_group = random.choice(item_group_table);
			
# 			row['financial_year'] = financial_year.lower();
# 			row['budget_type_seq_no'] = int(budget_type['budget_type_seq_no']);
# 			row['budget_type_code'] = str(budget_type['budget_type_code']).lower();
# 			row['budget_type_name'] = budget_type['budget_type_desc'].lower();
# 			row['object_seq_no'] = int(object_['obj_seq_no']);
# 			row['object_code'] = str(object_['obj_code']).lower();
# 			row['object_name'] = object_['obj_desc'].lower();
# 			row['item_group_seq_no'] = int(item_group['item_group_seq_no']);
# 			row['item_group_code'] = str(item_group['item_group_code']).lower();
# 			row['item_group_name'] = str(item_group['item_group_desc']).lower();

# 			row['first_allocation'] = random.randint(0, 99999)/100;
# 			row['additional_allocation'] = random.randint(0, 99999)/100;
# 			row['pending_amount'] = random.randint(0, 99999)/100;
# 			row['utilized_amount'] = random.randint(0, 99999)/100;
# 			row['liablity_amount'] = random.randint(0, 99999)/100;
# 			row['trans_in_amount'] = random.randint(0, 99999)/100;
# 			row['trans_out_amount'] = random.randint(0, 99999)/100;
# 			row['deduction_amount'] = random.randint(0, 99999)/100;
# 			row['current_actual_amount'] = random.randint(0, 99999)/100;
# 			row['total_allocation'] = row['first_allocation'] + row['additional_allocation'];
# 			row['balance_amount'] = row['first_allocation'] + row['additional_allocation'] - row['pending_amount'] - row['liablity_amount'] - row['utilized_amount'];

# 		elif page_type == 'procurement':
# 			item_type = random.choice(item_type_table);
# 			drug = random.choice(drug_table);
# 			item_packaging = random.choice(item_packaging_table);
# 			row['txn_year'] = end_date.split('-')[0].lower();
# 			row['txn_month'] = end_date.split('-')[1].lstrip('0').lower();
# 			row['txn_month_name'] = DateTime.getDateCategoryName(date=end_date, element='month').lower();
# 			row['item_type_seq_no'] = int(item_type['item_type_seq_no']);
# 			row['item_type'] = item_type['item_type_code'].lower();
# 			row['drug_seq_no'] = int(drug['drug_seq_no']);
# 			row['drug_name'] = drug['drug_name'].lower();
# 			row['drug_code'] = str(drug['drug_code']).lower();
# 			row['item_packaging_seq_no'] = int(item_packaging['item_packaging_seq_no']);
# 			row['item_packaging_name'] = item_packaging['item_packaging_name'].lower();

# 			row['e_p_approved_quantity'] = random.randint(0, 2000);
# 			row['min_unit_price'] = random.randint(0, 200)/100;
# 			row['max_unit_price'] = row['min_unit_price'];
# 			row['purchase_amount'] = row['e_p_approved_quantity'] * row['min_unit_price'];


# 		ptj = random.choice(ptjs);
# 		state = random.choice(states);
# 		facility = random.choice(facility_types);

# 		row['ptj_seq_no'] = int(ptj['ptj_seq_no']);
# 		row['ptj_code'] = str(ptj['ptj_code']).lower();
# 		row['ptj_name'] = ptj['ptj_name'].lower();

# 		if state_code:
# 			for idx in range(0, len(states)):
# 				ft = states[idx];
# 				if ft['state_code'] == state_code:
# 					row['state_seq_no'] = int(ft['state_seq_no']);
# 					row['state_code'] = str(ft['state_code']).lower();
# 					row['state_name'] = ft['state_name'].lower();
# 		else:
# 			row['state_seq_no'] = int(state['state_seq_no']);
# 			row['state_code'] = str(state['state_code']).lower();
# 			row['state_name'] = state['state_name'].lower();

# 		if facility_code:
# 			for idx in range(0, len(facility_types)):
# 				ft = facility_types[idx];
# 				if ft['facility_code'] == facility_code:
# 					row['facility_seq_no'] = int(ft['facility_seq_no']);
# 					row['facility_code'] = str(ft['facility_code']).lower();
# 					row['facility_name'] = ft['facility_name'].lower();
# 					row['facility_type'] = ft['facility_type'].lower();
# 					row['facility_prefix'] = '';
# 		else:
# 			row['facility_seq_no'] = int(facility['facility_seq_no']);
# 			row['facility_code'] = str(facility['facility_code']).lower();
# 			row['facility_name'] = facility['facility_name'].lower();
# 			row['facility_type'] = facility['facility_type'].lower();
# 			row['facility_prefix'] = '';
# 		# Logger.v('row unique_value b4', generateUniqueValue(row, page_type))
# 		# Logger.v('row unique_value', '_'.join(generateUniqueValue(row, page_type)))
# 		row['unique_value'] = '_'.join(generateUniqueValue(row, page_type)).lower();
# 		result.append(row);
# 	return result;

# def checkCrawlEnable():

# 	return;

# def save(data):
# 	today = DateTime.now(); # date with time
# 	# today = DateTime.toString(DateTime.now()); # date only
# 	folder_path = 'crawled_data/';
# 	if not os.path.exists(folder_path):
# 		os.makedirs(folder_path);
	
# 	for key in data:
# 		filename = '{0}/{1}_{2}'.format(folder_path, today, key);
# 		Logger.v('Saving', filename);
# 		fn.writeJSONFile(filename='{0}.json'.format(filename), data=data[key]);
# 		# fn.writeExcelFile(filename=filename, data=data[key]);
# 	Logger.v('Done saving.');

# def extractYear(data):
# 	year = [];
# 	for past_duration in data:
# 		# Logger.v('len(past_duration)', len(past_duration))
# 		for idx in range(len(past_duration)-1, -1, -1):
# 			pd = past_duration[idx];
# 			# Logger.v('pd', pd);
# 			y = DateTime.getDateCategoryName(date=pd, element='year', offset=8);
# 			if y not in year:
# 				year.append(y);
# 	return year;

# def retrieveOption(collection_name='state', show_keys=[], hide_keys=[]):
# 	dbManager = SharedMemoryManager.getInstance();
# 	db = dbManager.query();
# 	return_keys = {};
# 	for key in show_keys:
# 		return_keys.update({key: 1});
# 	for key in hide_keys:
# 		return_keys.update({key: 0});

# 	# Logger.v('return_keys', return_keys);
# 	if return_keys:
# 		result = list(db[collection_name].find({}, return_keys));
# 	else:
# 		result = list(db[collection_name].find({}));

# 	return result;

# def generateUniqueValue(data, collection_name, mode='dict'):
# 	result = [];
# 	for d in data:
# 		key = fn.camelToSnakecase(str=d);
# 		if key in collection_unique_keys[collection_name]:
# 			v = str(data[d]).strip();
# 			result.append(v.lower().replace(' ', '_'));
# 	return result;

# def generateUrl(url):
# 	subroute = url.split('/')[0];
# 	subroute_params = {
# 		'procurementdetails': '?stateCode={}&startdate={}&enddate={}&facilityCode={}',
# 		'budgetsummary': '?stateCode={}&financialYear={}&facilityCode={}',
# 	}
# 	base_url = '{}?api_key={}'.format(url, ''); #setup base url
# 	return base_url + '&startDate={}&endDate={}&pageSize={}&storeId={}&pageNumber={}';

# def updateReportData(params):
# 	report_name = fn.getNestedElement(params, 'keys.report');
# 	recordTime(key='update_mongo_start_time');
# 	createMongoIndexes(params);
# 	crawl_params = Schedule.generateCrawlParam(params);
# 	# Logger.v('crawl_params', crawl_params['budget']);
# 	result = {};
# 	file_list = generateFileList(data=crawl_params);
# 	for key in list(file_list.keys()):
# 		if key not in result:
# 			result[key] = [];
# 		filenames = file_list[key];
# 		combined_data_mongo = combineData(file_list=file_list, key=key);

# 	recordTime(key='update_mongo_end_time');
# 	# sendEmail(text=global_text, report=report_name);

# def recordTime(key):
# 	global global_text;
# 	new_key = key.replace('end', 'start');
# 	if new_key not in global_text:
# 		global_text[new_key] = DateTime.now(tzinfo=msia_tz);

# 	if new_key in global_text and not key == new_key:
# 		global_text[key] = DateTime.now(tzinfo=msia_tz);

# def sendEmail(text, report):
# 	keys = [
# 		'create_schedule_start_time',
# 		'create_schedule_end_time',
# 		'crawl_start_time',
# 		'crawl_end_time',
# 		'recrawl_1_start_time',
# 		'recrawl_1_end_time',
# 		'recrawl_0_start_time',
# 		'recrawl_0_end_time',
# 		'update_mongo_start_time',
# 		'update_mongo_end_time',
# 	];

# 	values = {};
# 	times = [];
# 	for key in keys:
# 		values[key] = fn.getNestedElement(text, key);
# 		if values[key]:
# 			times.append(datetime.timestamp(values[key]));

# 	print_text = 'Report: {0}:\n'.format(report);

# 	for key in keys:
# 		print_text += '{0}: {1}\n'.format(key, values[key]);

# 	time_taken = {};
# 	try:
# 		time_taken['update_mongo'] = fn.getNestedElement(values, 'update_mongo_end_time') - fn.getNestedElement(values, 'update_mongo_start_time');
# 	except Exception as ex:
# 		Logger.v('update mongo', ex);
# 	try:
# 		time_taken['crawl'] = fn.getNestedElement(values, 'crawl_end_time') - fn.getNestedElement(values, 'create_schedule_start_time');
# 	except Exception as ex:
# 		Logger.v('crawl', ex);
# 	try:
# 		time_taken['recrawl'] = fn.getNestedElement(values, 'recrawl_1_end_time') - fn.getNestedElement(values, 'recrawl_0_start_time');
# 	except Exception as ex:
# 		Logger.v('recrawl', ex);
# 	try:
# 		Logger.v('total start', datetime.fromtimestamp(times[0]));
# 		Logger.v('total end', datetime.fromtimestamp(times[-1]));
# 		time_taken['total'] = datetime.fromtimestamp(times[-1]) - datetime.fromtimestamp(times[0]);
# 	except Exception as ex:
# 		Logger.v('total', ex);

# 	print_text += '\nTime Taken:\n';

# 	for key in time_taken:
# 		print_text += '{0}: {1}\n'.format(key, time_taken[key])
# 	Logger.v('send email, print_text', print_text);
# 	Mail.send('{0} Crawl - Store {1}'.format(DateTime.getReadableDate(DateTime.now()), report), print_text);

# def combineData(file_list, key):
# 	dbManager = SharedMemoryManager.getInstance();
# 	db = dbManager.query();
# 	filenames = file_list[key];
# 	iteration = 0;
# 	if filenames:
# 		fn.printProgressBar(iteration=0, total=len(filenames));
# 		for idx in range(0, len(filenames)):
# 			iteration += 1;
# 			fn.printProgressBar(iteration=iteration, total=len(filenames));
# 			raw = File.readJson(filename=filenames[idx]);
# 			Logger.v('Length of Raw Data:', len(raw));

# 			for idx1 in range(0, len(raw)):
# 				row = raw[idx1];
# 				report_query = {
# 					'budget': {
# 						'financial_year': fn.getNestedElement(row, 'financial_year'), 
# 						'budget_type_seq_no': fn.getNestedElement(row, 'budget_type_seq_no'), 
# 						'object_seq_no': fn.getNestedElement(row, 'object_seq_no'), 
# 						'item_group_seq_no': fn.getNestedElement(row, 'item_group_seq_no'), 
# 						'ptj_seq_no': fn.getNestedElement(row, 'ptj_seq_no'), 
# 						'facility_seq_no': fn.getNestedElement(row, 'facility_seq_no'), 
# 					},
# 					'procurement': {
# 						'txn_year': fn.getNestedElement(row, 'txn_year'), 
# 						'txn_month': fn.getNestedElement(row, 'txn_month'), 
# 						'drug_seq_no': fn.getNestedElement(row, 'drug_seq_no'), 
# 						'ptj_seq_no': fn.getNestedElement(row, 'ptj_seq_no'), 
# 						'facility_seq_no': fn.getNestedElement(row, 'facility_seq_no'), 
# 					},
# 				};
# 				query = report_query[key];
# 				found_uv = list(db[key].find(query));
# 				if found_uv:
# 					dbManager.addBulkUpdate(key, query, row, batch=True);
# 				else:
# 					dbManager.addBulkInsert(key, row, batch=True);

# 			# if idx == 1:
# 			# 	break;
		
# 	dbManager.executeBulkOperations(None);
# 	return True;

# def generateFileList(data):
# 	result = {};
# 	directory_list = generateDirectoryList(data=data);
# 	for key in list(directory_list.keys()):
# 		Logger.v('key', key);
# 		if key not in result:
# 			result[key] = [];
# 		directories = directory_list[key];
# 		for idx in range(0, len(directories)):
# 			directory = directories[idx];
# 			try:
# 				latest_files = File.getLatestFile(directory=directory, extension='.json');
# 				result[key].append(latest_files);
# 			except Exception as e:
# 				Logger.v(e);

# 	# Logger.v('result', result);
# 	return result;

# def generateDirectoryList(data):
# 	directory_list = {};
# 	for rk in list(data.keys()):
# 		Logger.v('rk', rk);
# 		# Logger.v('data', data)
# 		if rk not in directory_list:
# 			directory_list[rk] = [];
# 		# for idx in range(len(data[rk])-1, len(data[rk])-3, -1): # from earliest to latest, take first 2
# 		for idx in range(len(data[rk])-1, -1, -1): # from earliest to latest
# 		# for idx in range(0, len(data[rk])): # from latest to earliest
# 			row = data[rk][idx];

# 			page_type = fn.getNestedElement(row, 'page_type', '');
# 			state_code = fn.getNestedElement(row, 'state_code', '');
# 			financial_year = fn.getNestedElement(row, 'financial_year', '');
# 			facility_code = fn.getNestedElement(row, 'facility_code', 'all_facility');
# 			start_date = fn.getNestedElement(row, 'start_date', '');
# 			end_date = fn.getNestedElement(row, 'end_date', '');
# 			# Logger.v('vairable', page_type)
# 			folder_path_map = {
# 				'budget': '/'.join([crawl_folder, page_type, 'year_{0}'.format(financial_year), 'state_{0}'.format(state_code), facility_code]),
# 				'procurement': '/'.join([crawl_folder, page_type, 'year_{0}'.format(start_date[:4]), 'state_{0}'.format(state_code), '_'.join([start_date[:-3], end_date[:-3]]), facility_code]),
# 			};

# 			folder_path = folder_path_map[rk];
# 			# Logger.v('folder_path', folder_path);
# 			directory_list[rk].append(folder_path);


# 	# Logger.v('directory_list', directory_list);
# 	return directory_list;

# def createMongoIndexes(params):
# 	dbManager = SharedMemoryManager.getInstance();
# 	db = dbManager.query();
# 	reports = fn.getNestedElement(params, 'keys.report', ['budget', 'procurement']);
# 	# report_index = {
# 	# 	'budget': {
# 	# 		'index': [('financial_year', 1), ('budget_type_seq_no', 1), ('object_seq_no', 1), ('item_group_seq_no', 1), ('ptj_seq_no', 1), ('facility_seq_no', 1)],
# 	# 		'name': 'financial_year_1_budget_type_seq_no_1_object_seq_no_1_item_group_seq_no_1_ptj_seq_no_1_facility_seq_no_1',
# 	# 	},
# 	# 	'procurement': {
# 	# 		'index': [('txn_year', 1), ('txn_month', 1), ('drug_seq_no', 1), ('ptj_seq_no', 1), ('facility_seq_no', 1)],
# 	# 		'name': 'txn_year_1_txn_month_1_drug_seq_no_1_ptj_seq_no_1_facility_seq_no_1',
# 	# 	},
# 	# };
# 	report_index = {
# 		'budget': [
# 			{
# 				'index': [('financial_year', -1), ('budget_type_seq_no', 1), ('object_seq_no', 1), ('item_group_seq_no', 1), ('ptj_seq_no', 1), ('facility_seq_no', 1)],
# 				'name': 'financial_year_-1_budget_type_seq_no_1_object_seq_no_1_item_group_seq_no_1_ptj_seq_no_1_facility_seq_no_1',
# 			},
# 			{
# 				'index': [('financial_year', -1), ('budget_type_code', 1), ('facility_type', 1), ('facility_code', 1), ('ptj_code', 1), ('state_code', 1)],
# 				'name': 'financial_year_-1_budget_type_code_1_facility_type_1_facility_code_1_ptj_code_1_state_code_1',
# 			},
# 		],
# 		'procurement': [
# 			{
# 				'index': [('txn_year', -1), ('txn_month', 1), ('drug_seq_no', 1), ('ptj_seq_no', 1), ('facility_seq_no', 1)],
# 				'name': 'txn_year_-1_txn_month_1_drug_seq_no_1_ptj_seq_no_1_facility_seq_no_1',
# 			},
# 			{
# 				'index': [('txn_year', -1), ('txn_month', 1), ('facility_type', 1), ('facility_code', 1), ('ptj_code', 1), ('state_code', 1), ('item_type', 1)],
# 				'name': 'txn_year_-1_txn_month_1_facility_type_1_facility_code_1_ptj_code_1_state_code_1_item_type_1',
# 			},
# 		],
# 	};

# 	for report in reports:
# 		Logger.v('report:', report);
# 		col = db[report];
# 		index_info = col.index_information();
# 		indexes = report_index[report];
# 		for idx in indexes:
# 			index_name = idx['name'];
# 			index = idx['index'];
# 			if index_name not in index_info:
# 				Logger.v('Creating index for', report);
# 				col.create_index(index);

# 	# db['budget'].drop_index('financial_year_-1_budget_type_code_1_facility_type_1_facility_code_1_ptj_code_1_state_code_1'); #XXX
# 	# db['procurement'].drop_index('txn_year_-1_txn_month_1_facility_type_1_facility_code_1_ptj_code_1_state_code_1_item_type_1'); #XXX

# 	return True;



