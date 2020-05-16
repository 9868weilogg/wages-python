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

global_check_data = {};
global_text = {};
global_start_count = 0;

def run(params, data):
	missing_dates = getMissingDates(data);

	for rk in missing_dates:
		dates = missing_dates[rk];
		Logger.v('missing dates:', dates);
		if dates:
			Logger.v('------recrawl------', rk)
			for date in dates:
				try:
					params['schedule_params']['today'] = date;
					params['keys']['report'] = [rk];
					Schedule.run(params)
				except Exception as e:
					Logger.v('Main.recrawl:', e);

def checkEmpty(params):
	global global_check_data;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	custom_params = copy.deepcopy(params);
	report_keys = fn.getNestedElement(params, 'keys.report', ['procurement', 'budget']);
	interval = fn.getNestedElement(params, 'interval', 1);
	past_dates = DateTime.getPastDate(count=12, duration=interval); # check previous 12 month data
	year = Crawl.extractYear(data=past_dates[0]);
	first_date = past_dates[0][-1][0];
	last_date = past_dates[0][0][1];
	# Logger.v('first_date', first_date, 'last_date', last_date);
	state_by = 'state_code';
	states = list(db['state'].find({},{'_id': 0, state_by: 1}));
	result = {};
	datetime = DateTime.toString(DateTime.now(tzinfo=msia_tz), date_format='%Y-%m-%d-%H-%M-%S');

	custom_params['first_date'] = first_date;
	custom_params['last_date'] = last_date;
	custom_params['state_by'] = state_by;
	custom_params['states'] = states;
	temp_result = generateTemplate(params=custom_params);

	for rk in report_keys:
		if rk not in global_check_data:
			global_check_data[rk] = [];

		for y in year:
			root_path = '{0}/{1}/year_{2}'.format(crawl_folder, rk, y);
			openDir(root_path, rk);
			for gcd in global_check_data[rk]:
				date = gcd.split('_')[0];
				state = gcd.split('_')[1];
				if DateTime.inrange(date, [first_date, last_date]):
					try:
						temp_result[rk][date][state] += 1;
					except Exception as e:
						# Logger.v('Main.checkEmpty:', e);
						pass;

	for rk in temp_result:
		if rk not in result:
			result[rk] = [];
		for date in temp_result[rk]:
			result[rk].append(temp_result[rk][date]);

		filename = '{0}/{1}_check_moh_empty'.format(test_folder, rk);
		# filename = 'tests/{0}_{1}_check_moh_empty'.format(rk, datetime);
		fn.writeExcelFile(filename=filename, data=result[rk]);
	global_check_data = {};
	return result;

def generateTemplate(params):
	result = {};
	report_keys = fn.getNestedElement(params, 'keys.report', ['procurement', 'budget']);
	first_date = fn.getNestedElement(params, 'first_date');
	last_date = fn.getNestedElement(params, 'last_date');
	state_by = fn.getNestedElement(params, 'state_by');
	states = fn.getNestedElement(params, 'states');
	today = DateTime.now(tzinfo=msia_tz); # date only
	for rk in report_keys:

		if rk not in result:
			result[rk] = {};
		
		for date in DateTime.getBetween([first_date, last_date], element='date')['order']:
			end_date_of_month = DateTime.getDaysAgo(days_to_crawl=1, datefrom=DateTime.getNextMonth(DateTime.convertDateTimeFromString(date)));
			year_month = date[:7];
			day_diff = DateTime.getDifferenceBetweenDuration([today, end_date_of_month]);

			if day_diff >= 0:
				date_str = DateTime.toString(today);
			else:
				date_str = DateTime.toString(end_date_of_month);

			if date_str not in result[rk]:
				result[rk][date_str] = {};

			result[rk][date_str].update({
				'date': date_str,
			})
			for idx in range(0, len(states)):
				state = states[idx][state_by];
				result[rk][date_str].update({
					state: 0,
				});
	return result;

def getMissingDates(data):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	missing_dates = {};
	today = DateTime.now(tzinfo=msia_tz); # date only
	state_by = 'state_code';
	states = list(db['state'].find({},{'_id': 0, state_by: 1}));
	current_year = DateTime.getDateCategoryName(date=DateTime.now(tzinfo=msia_tz), element='year');
	for rk in data:
		row = data[rk];
		if rk not in missing_dates:
			missing_dates[rk] = [];

		dates = groupDates(params={'states': states, 'state_by': state_by}, data=row);
		for date in dates['missing']:
			end_date_of_month = DateTime.getDaysAgo(days_to_crawl=1, datefrom=DateTime.getNextMonth(DateTime.convertDateTimeFromString(date)));
			day_diff = DateTime.getDifferenceBetweenDuration([today, end_date_of_month]);

			if day_diff >= 0:
				date_str = DateTime.toString(today);
			else:
				date_str = DateTime.toString(end_date_of_month);

			if date_str not in dates['crawled']:
				missing_dates[rk].append(date_str);

			# Logger.v('day_diff', day_diff);
			# Logger.v('date', DateTime.getDaysAgo(days_to_crawl=1, datefrom=DateTime.getNextMonth(DateTime.convertDateTimeFromString(ed))));
		missing_dates[rk] = sorted(list(set(missing_dates[rk])), reverse=True);
	return missing_dates;

def groupDates(params, data):
	states = fn.getNestedElement(params, 'states');
	state_by = fn.getNestedElement(params, 'state_by');
	missing_list = [];	
	crawled_list = [];
	for state in states:
		filtered_data = list(filter(lambda d: d[state[state_by]] == 0, data));
		if filtered_data:
			missing_list += filtered_data;

		c_filtered_data = list(filter(lambda d: not d[state[state_by]] == 0, data));
		if c_filtered_data:
			crawled_list += c_filtered_data;

	return {
		'crawled': sorted(list(set([date['date'] for date in crawled_list])), reverse=True),
		'missing': sorted(list(set([date['date'] for date in missing_list])), reverse=True),
	};

def openDir(root, report):	
	global global_check_data;
	# Logger.v('root', root)
	try:
		for dir_ in sorted(os.listdir(root), reverse=True):
			path = '/'.join([root, dir_]);
			# Logger.v('path', path)
			if os.path.isdir(path):
				# Logger.v('dir', dir_)
				openDir(path, report);
			else:
				# Logger.v('path', path)
				recordCrawledFile(data={'path': path, 'report': report});
	except Exception as ex:
		Logger.v('Crawl.Recrawl.openDir', ex);

def recordCrawledFile(data):
	global global_check_data;
	path = fn.getNestedElement(data, 'path');
	report = fn.getNestedElement(data, 'report');
	split_path = path.split('/');
	file = split_path[-1];
	date = file.split('.')[0];
	year = date.split('-')[0];
	if file == '.json':
		os.remove(path);
		Logger.v('Removed', path);
	indexes = {
		'budget': {
			'year': -4,
			'state': -3,
			'all_facility': -2,
		},
		'procurement': {
			'year': -5,
			'state': -4,
			'all_facility': -2,
		},
	};
	state_idx = indexes[report]['state'];
	year_idx = indexes[report]['year'];
	all_facility_idx = indexes[report]['all_facility'];

	conditions = {
		'budget': split_path[year_idx] == 'year_{0}'.format(year),
		'procurement':  True,
	};
	
	if not file in ['.DS_Store', '.json'] and split_path[all_facility_idx] == 'all_facility' and conditions[report]:
		state_code = split_path[state_idx].replace('state_', '');
		global_check_data[report].append('_'.join([date, state_code]));
		# Logger.v('break')
