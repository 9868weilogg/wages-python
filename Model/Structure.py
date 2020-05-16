from lib import fn;
from lib import Logger;
from lib import DateTime;
from lib import SharedMemoryManager;
from lib import DebugManager;
from Model import Stock as ModelStock;
from Model import Facility as ModelFacility;

import copy;
import pytz;

msia_tz = pytz.timezone('Asia/Kuala_Lumpur');
date_retrieve_limit = 7;

def stockCheck(params, data):
	process_order = fn.getNestedElement(params, 'process_order');
	filter_quantity = fn.getNestedElement(params, 'filter.quantity', []);
	total_po_length = len(process_order);
	result = [];
	idx_count = 0;
	for level_0 in data:
		level_0_data = data[level_0];
		if idx_count < total_po_length - 1:
			idx_count += 1;
			poidx1 = process_order[idx_count]; 
			portion_1 = [];
			for level_1 in fn.getNestedElement(level_0_data, poidx1, []):
				level_1_data = level_0_data[poidx1][level_1];
				if idx_count < total_po_length - 1:
					idx_count += 1;
					poidx2 = process_order[idx_count]; 
					portion_2 = [];
					for level_2 in level_1_data[poidx2]:
						level_2_data = level_1_data[poidx2][level_2];
						if idx_count < total_po_length - 1:
							idx_count += 1;
							poidx3 = process_order[idx_count]; 
							portion_3 = [];
							for level_3 in level_2_data[poidx3]:
								level_3_data = level_2_data[poidx3][level_3];

								filterQuantity(params=params, data={'main': portion_3, 'sub': level_3_data});
							addData(params={'key': poidx3, 'process_order': process_order}, data={'main': portion_2, 'sub': portion_3, 'misc': level_2_data});
							idx_count -= 1;
						else:
							filterQuantity(params=params, data={'main': portion_2, 'sub': level_2_data});
					addData(params={'key': poidx2, 'process_order': process_order}, data={'main': portion_1, 'sub': portion_2, 'misc': level_1_data});
					idx_count -= 1;
				else:
					filterQuantity(params=params, data={'main': portion_1, 'sub': level_1_data});
			addData(params={'key': poidx1, 'process_order': process_order}, data={'main': result, 'sub': portion_1, 'misc': level_0_data});
			idx_count -= 1;
	result = stockCheckSummary(params=params, data=result);
	return result;

def stockCheckSummary(params, data):
	process_order = fn.getNestedElement(params, 'process_order');
	result = copy.deepcopy(data);
	# Logger.v('result', result);
	idx_count = 0;
	total_po_length = len(process_order);
	for idx in range(0, len(result)):
		if idx_count < total_po_length - 1:
			idx_count += 1;
			row = result[idx];
			# Logger.v('row', row);
			key = renameDictKey(key=process_order[idx_count], process_order=process_order);
			level_1_data = row[key];
			combine_data = {};
			for idx1 in range(0, len(level_1_data)):
				if idx_count < total_po_length - 1:
					idx_count += 1;
					row1 = level_1_data[idx1];
					# Logger.v('row1', row1);
					key = renameDictKey(key=process_order[idx_count], process_order=process_order);
					level_2_data = row1[key];
					for idx2 in range(0, len(level_2_data)):
						row2 = level_2_data[idx2];
						# Logger.v('row2', row2);
						quantity = row2['quantity'];
						unique_id = '_'.join([row2['code'], row2['sku']]);
						if unique_id not in combine_data:
							combine_data[unique_id] = copy.deepcopy(row2);
						else:
							combine_data[unique_id]['quantity'] = combine_data[unique_id]['quantity'] + quantity;
					idx_count -= 1;

			if combine_data:
				row['summary'] = [combine_data[cd] for cd in combine_data];
			else:
				row['summary'] = row[key];
			row['summary'] = sorted(row['summary'], key=lambda k: k['name'], reverse=False);
			idx_count -= 1;
	# Logger.v('result', result)
	return result;

def addData(params, data):
	key = fn.getNestedElement(params, 'key');
	process_order = fn.getNestedElement(params, 'process_order');
	main = fn.getNestedElement(data, 'main');
	sub = fn.getNestedElement(data, 'sub');
	misc = fn.getNestedElement(data, 'misc');
	if sub:
		portion_key = renameDictKey(key=key, process_order=process_order);
		main.append({
			'id': misc['id'],
			'name': misc['name'],
			'code': misc['code'],
			portion_key: sub,
		});

def filterQuantity(params, data):
	filter_quantity = fn.getNestedElement(params, 'filter.quantity', None);
	main = fn.getNestedElement(data, 'main');
	sub = fn.getNestedElement(data, 'sub');
	qty = sub['quantity'];
	can_append = ModelStock.quantityWithinRange(params=params, quantity=qty);
	if can_append or not filter_quantity:
		main.append(sub);

def renameDictKey(key, process_order):
	if key == process_order[-1]:
		return 'drug';
	else:
		return key.replace('_code', '');

def stockIntegrity(params, data):
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