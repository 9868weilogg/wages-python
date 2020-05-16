from lib import SharedMemoryManager;
from lib import DebugManager;
from lib import fn;
from lib import Logger;
from lib import DateTime;
from lib import File;

from Model import StockIssueIntegrity as ModelSIIntegrity;
from Model import Structure as ModelStructure;
from Model import Facility as ModelFacility;
import pytz;

msia_tz = pytz.timezone('Asia/Kuala_Lumpur');
date_retrieve_limit = 7;

def check(params):
	data = ModelSIIntegrity.check(params);
	result = toOutputStructure(params=params, data=data);
	return result;

def toOutputStructure(params, data):
	Debug = DebugManager.DebugManager();
	Debug.start();
	global msia_tz, date_retrieve_limit;
	result = [];
	today = DateTime.now(tzinfo=msia_tz);
	start_date = DateTime.getDaysAgo(date_retrieve_limit, datefrom=today);
	durations = DateTime.getBetween([start_date, today], element='date', offset=24)['order']; # offset 24 hour to include today
	state_data = fn.getNestedElement(data, 'state');
	facility_data_by_state = fn.getNestedElement(data, 'state_facility');

	check_data = combinedFacilityList(data=facility_data_by_state);
	result = getIntegrity(
		params={
			'durations': durations,
		}, 
		data={
			'facility': facility_data_by_state,
			'state': state_data,
			'to_update': result,
			'check_data': check_data,
		}
	);
	updateStateData(result);
	result = list(sorted(result, key=lambda k: k['name'], reverse=False));
	Debug.end();
	Debug.show('Model.Structure.stockIntegrity');
	return result;

def combinedFacilityList(data):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	result = ModelFacility.getActiveFacility();
	for state_code in data:
		for facility_code in data[state_code]:
			state_facility_exist = list(filter(lambda x:x['state_name'] == state_code and x['facility_code'] == facility_code, result));
			# Logger.v('state_facility_exist', state_facility_exist, state_code, facility_code, data[state_code][facility_code]['name']);
			if not state_facility_exist:
				result.append({
					'state_code': state_code, 
					'state_name': state_code, 
					'ptj_code': '', 
					'ptj_name': '', 
					'facility_code': facility_code, 
					'facility_name': data[state_code][facility_code]['name'], 
					'facility_type': '', 
					'active': 'a'
				});
	return result;

def getIntegrity(params, data):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	check_data = fn.getNestedElement(data, 'check_data')
	facility = ModelFacility.getActiveFacility();
	filter_key = fn.getNestedElement(params, 'filter_key');
	durations = fn.getNestedElement(params, 'durations');
	result = fn.getNestedElement(data, 'to_update');
	state_data = fn.getNestedElement(data, 'state');
	facility_data_by_state = fn.getNestedElement(data, 'facility');
	data_list = getFacilityByState(params=params, data=check_data);

	for key in data_list:
		row = fn.getNestedElement(data_list, key);
		count = getTotalCount(params={'filter_key': filter_key, 'key': key}, data={'row': row, 'facility': facility});
		obj_ = {
			'id': fn.convertToSnakecase(fn.getNestedElement(row, 'id')),
			'name': fn.getNestedElement(row, 'name'),
			'code': fn.getNestedElement(row, 'code'),
			'data': [],
		}
		for idx in range(len(durations)-1, -1, -1):
			date = durations[idx];
			previous_date = DateTime.toString(DateTime.getDaysAgo(1, datefrom=date));
			# Logger.v('date', date, 'previous_date', previous_date);
			if filter_key:
				date_count = fn.getNestedElement(facility_data_by_state, '{0}.{1}.{2}'.format(filter_key, key, date), 0);
				if not date_count:
					date_count = 0;
			else:
				date_count = 0; # do not include those positive, count missing facility quantity only
				# date_count = fn.getNestedElement(state_data, '{0}.{1}'.format(key, date), 0);
			if filter_key:
				val = date_count - count;
			else:
				val = 0;
			obj_['data'].append({
				previous_date: val, # negative value is missing, 0 mean complete, positive value is not found from user upload facility	
			})
		if filter_key:
			# Logger.v('recursive end')
			pass;

		else:
			obj_['facility'] = [];
			obj_['facility'] = getIntegrity(params={
				'filter_key': key,
				'durations': durations,
			}, data={
				'state': state_data,
				'facility': facility_data_by_state,
				'to_update': obj_['facility'],
				'check_data': check_data,
			});
		result.append(obj_);

	# Logger.v('result', result)
	return result;

def updateStateData(data):
	for idx in range(0, len(data)):
		row = data[idx];
		state = row['code'];
		# Logger.v('state', state);
		missing_count = {};
		for d in row['data']:
			# Logger.v('d', list(d.keys())[0])
			date = list(d.keys())[0];
			missing_count[date] = 0;
		row['facility'] = list(sorted(row['facility'], key=lambda k: k['name'], reverse=False));
		for idx1 in range(0, len(row['facility'])):
			row1 = row['facility'][idx1];
			for d1 in row1['data']:
				date2 = list(d1.keys())[0];
				missing_value = list(d1.values())[0];
				# Logger.v('d1', missing_value, missing_value < 0)
				if missing_value < 0:
					missing_count[date2] += missing_value;
		# Logger.v('missing_count', missing_count);
		for d in row['data']:
			date = list(d.keys())[0];
			d[date] = missing_count[date];
	return data;

def getFacilityByState(params, data):
	filter_key = fn.getNestedElement(params, 'filter_key');
	result = {};
	if filter_key:
		# use this after confirm facility file structure
		# filtered = list(filter(lambda x: x['state_code'] == filter_key, data));
		#XXX Temporary use state_name as state_code, follow stock data
		filtered = list(filter(lambda x: x['state_name'] == filter_key, data));
	else:
		filtered = data;
	
	for idx in range(0, len(filtered)):
		row = filtered[idx];
		state_name = row['state_name'];
		# use this after confirm facility file structure
		# state_code = str(row['state_code']); 
		#XXX Temporary use state_name as state_code, follow stock data
		state_code = str(row['state_name']); 
		facility_code = str(row['facility_code']);
		facility_name = row['facility_name'];
		temp = {
			'state': {},
			'facility': {},
		}
		if filter_key:
			temp_key = 'facility';
			temp[temp_key]['id'] = '_'.join([state_code, facility_code]);
			temp[temp_key]['name'] = ModelFacility.facilityNameShorten(facility_name);
			temp[temp_key]['code'] = facility_code;
		else:
			temp_key = 'state';
			temp[temp_key]['id'] = state_code;
			temp[temp_key]['name'] = state_name;
			temp[temp_key]['code'] = state_code;

		if temp[temp_key]['code'] not in result:
			result[temp[temp_key]['code']] = {
				'id': temp[temp_key]['id'],
				'name': temp[temp_key]['name'],
				'code': temp[temp_key]['code'],
			};
	return result;

def getTotalCount(params, data):
	filter_key = fn.getNestedElement(params, 'filter_key');
	key = fn.getNestedElement(params, 'key');
	row = fn.getNestedElement(data, 'row');
	facility = fn.getNestedElement(data, 'facility');
	if facility:
		if filter_key:
			state_code = filter_key;
			facility_code = key;
			# use this after confirm facility file structure
			# count = len(list(filter(lambda x: x['state_code'] == state_code and x['facility_code'] == facility_code, facility))); 
			#XXX Temporary use state_name as state_code, follow stock data
			count = len(list(filter(lambda x: x['state_name'] == state_code and x['facility_code'] == facility_code, facility))); 
		else:
			state_code = key;
			# use this after confirm facility file structure
			# count = len(list(filter(lambda x: x['state_code'] == state_code, facility))); 
			#XXX Temporary use state_name as state_code, follow stock data
			count = len(list(filter(lambda x: x['state_name'] == state_code, facility))); 
	else:
		count = fn.getNestedElement(row, 'count', 0);
	return count;