from lib import DebugManager;
from lib import SharedMemoryManager;
from lib import Logger;
from lib import fn;
from lib import File;
from Model import Stock as ModelStock;
from Report import Stock as ReportStock;
import asyncio;
import copy;

facility_by_state_collection_name = 'facility_by_state';
unique_facility = [];
column_keymap = {
	'STATE_CODE': 'state_code',
	'STATE_NAME': 'state_name',
	'PTJ_CODE': 'ptj_code',
	'PTJ_NAME': 'ptj_name',
	'FACILITY_CODE': 'facility_code',
	'FACILITY_NAME': 'facility_name',
	'FACILITY_TYPE': 'facility_type',
	'Active': 'active',
};
facility_group_mapping = {
	'hos': 'hospital',
	'kd': 'clinic',
	'kk': 'clinic',
	'kk1m': 'clinic',
	'musn/pbfn': 'pkd',
	'pkd': 'pkd',
};
short_name_mapping = {
	'hospital': 'hos',
	'klinik desa': 'kd',
	'klinik kesihatan': 'kk',
	'klinik komuniti': 'kk1m',
	'pejabat kesihatan daerah': 'pkd',
};

def upload(params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	path = fn.getNestedElement(params, 'path');
	processed_filename = File.converExcelFileToCsv(path, ignore_index=True, overwrite=True);

	reset();
	File.readCsvFileInChunks(processed_filename, save, params, chunksize=2500);
	Debug.trace('uploaded facility to mongo.');
	ReportStock.triggerOnComplete(params);
	Debug.trace('trigger api on complete.');
	Debug.end();
	Debug.show('Model.Facility.upload');


@asyncio.coroutine
def save(params, chunk, chunks_info):
	global facility_by_state_collection_name, column_keymap, unique_facility;
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
		obj_ = transformDataToLower(row);
		facility_group = facilityGroupMapping(facility_type=obj_['facility_type']);
		facility_short_name = facilityNameShorten(facility_name=obj_['facility_name']);
		obj_.update({
			'facility_group': facility_group,
			'facility_short_name': facility_short_name,
		});
		state_facility_code = '_'.join([str(obj_['state_code']), str(obj_['facility_code'])]);
		if state_facility_code not in list(set(unique_facility)):
			dbManager.addBulkInsert(facility_by_state_collection_name, obj_, batch=True);
			unique_facility.append(state_facility_code);
		# fn.printProgressBar(current_index+idx, total_index, 'Processing Item Insertion');
	
	#ensure all data is save properly
	dbManager.executeBulkOperations(facility_by_state_collection_name);
	return chunks_info;

def facilityGroupMapping(facility_type):
	global facility_group_mapping;
	facility_group = fn.getNestedElement(facility_group_mapping, facility_type, 'null');
	return facility_group;

def facilityNameShorten(facility_name):
	global short_name_mapping;
	facility_short_name = copy.deepcopy(facility_name);
	for facility in short_name_mapping:
		facility_short_name = facility_short_name.replace(facility, short_name_mapping[facility]);
		if not facility_name == facility_short_name:
			# Logger.v('facility_name', facility_name, 'facility_short_name', facility_short_name);
			break;
	return facility_short_name;

def reset():
	global facility_by_state_collection_name, unique_facility;
	unique_facility = [];
	dbManager = SharedMemoryManager.getInstance();
	dbManager.dropCollection(facility_by_state_collection_name);

def transformDataToLower(data): # key to snakecase, value to lowercase
	result = {};
	for row_key in data:
		row_value = data[row_key];
		new_key = column_keymap[row_key].replace(' ', '_').lower();
		if type(row_value) == str:
			new_value = row_value.lower();
		else:
			new_value = row_value;
		result[new_key] = new_value;
	return result;

def getFacilityCodeList(facility_group):
	global facility_by_state_collection_name;
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	facilities = list(db[facility_by_state_collection_name].find({'facility_group': {'$in': [c.lower() for c in facility_group]}}, {'_id': 0, 'inserted_at': 0, 'updated_at': 0}));
	facility_code_list = [];
	for idx in range(0, len(facilities)):
		row = facilities[idx];
		code = row['facility_code'];
		facility_code_list.append(code);
	return facility_code_list;

def getActiveFacility():
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	facility_list = list(db[facility_by_state_collection_name].find({'active': 'a'}, {'_id': 0, 'inserted_at': 0, 'updated_at': 0}));
	return facility_list;

def get(params):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	facility_list = list(db[facility_by_state_collection_name].find({}, {'_id': 0, 'inserted_at': 0, 'updated_at': 0}));
	return facility_list;