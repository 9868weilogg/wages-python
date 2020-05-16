from lib import *;
import math;
import copy;
import json;
from bson.objectid import ObjectId;
from Report import File;

CRAWL_TYPE = fn.getNestedElement(fn.config,'CRAWLER_TYPE','budget,procurement').split(',');
collection_name = 'crawl_queue';
QUERY_MODE = ['renew','priority', 'expired', 'next'];
EXPIRATION_DURATION = 2; #duration in minute
def extractParams(page):
	upid = fn.getNestedElement(page, 'upid');
	page_type = fn.getNestedElement(page, 'page_type');

	return (upid, page_type);

def isRunning(page, extra=None, admin=False):
	upid, page_type = extractParams(page);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	q = { 	'page_type':page_type, 
			'upid':upid, 
			'running_at':{'$ne':None}, 
			'completed_at':None, 
		};
	if admin:
		q['page_access_token'] = {'$ne':None};
	if extra:
		q.update(extra);
	previous = db[collection_name].find_one(q);
	return previous != None;

def isPending(page, admin=False):
	upid, page_type = extractParams(page);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	q = { 'page_type':page_type, 
			'upid':upid, 
			'running_at': None, 
			'completed_at':None, 
		};
	if admin:
		q['page_access_token'] = {'$ne':None};
	previous = db[collection_name].find_one(q);
	return previous != None;

def getMaximumDuration(page):
	upid, page_type = extractParams(page);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	previous = db[collection_name].find_one({ 'page_type':page_type, 
											'upid':upid,
		}, sort=[('duration', -1)]);
	# Logger.v(previous);
	if previous:
		return fn.getNestedElement(previous, 'duration', 0) ;
	return 0;
def hasCompleted(page):
	upid, page_type = extractParams(page);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	previous = db[collection_name].find_one({ 'page_type':page_type, 
											'upid':upid, 
											'running_at':{'$ne':None}, 
											'completed_at':{'$ne':None}
		});
	return previous != None;

def start(qid):
	ip = basic.getCurrentIP();
	dbManager = SharedMemoryManager.getInstance();
	dbManager.addBulkUpdate(collection_name, { '_id': qid }, 
							{ 'running_at': DateTime.now(), 'running_host': ip }, 
							batch=False);
	return True;

def end(qid):
	dbManager = SharedMemoryManager.getInstance();
	dbManager.addBulkUpdate(collection_name, { '_id': qid }, 
							{ 'completed_at': DateTime.now() }, 
							batch=False);
	return True;

def reset(qid):
	if type(qid) == str:
		qid = ObjectId(qid);
	ip = basic.getCurrentIP();
	dbManager = SharedMemoryManager.getInstance();
	dbManager.addBulkUpdate(collection_name, { '_id': qid }, 
							{ 'running_at': None, 'running_host': None, 'completed_at':None, 'duration' : int(fn.getNestedElement(fn.config,'CRAWL_DURATION', '12')) }, 
							batch=False);
	return True;

def generateNextQuery(extra=None, mode='next', exclude=[]):
	global CRAWL_TYPE, QUERY_MODE, EXPIRATION_DURATION;
	target_crawl_type = list(filter(lambda x: x not in exclude, CRAWL_TYPE));
	query = {'completed_at':None, 'running_at':None};
	if not mode in QUERY_MODE:
		Logger.e('Queue.generateNextQuery: Mode %s Not Found.'%mode);
	if CRAWL_TYPE:
		query.update({'page_type':{
			'$in': target_crawl_type
		}});# ['ig'] target instagram only.

	if mode == 'renew':
		query.update({
			'priority':'renew'
		})
	elif mode == 'priority':
		query.update({
			'priority':'new'
		});
	elif mode == 'expired':
		query.update({ #crawl should not more than 2 minute
			'running_at':{ '$lte': DateTime.getMinutesAgo(EXPIRATION_DURATION) } 
		});
	if extra:
		query.update(extra);
	return query;

# def getNextExpired():
# 	dbManager = SharedMemoryManager.getInstance();
# 	db = dbManager.query();
# 	query = generateNextQuery({
		
# 	});
# 	nextQueue = db[collection_name].find_one(query);
# 	return nextQueue;

# def getNextPriority():
# 	dbManager = SharedMemoryManager.getInstance();
# 	db = dbManager.query();
# 	query = generateNextQuery({
# 		'priority':'new'
# 	});
# 	nextQueue = db[collection_name].find_one(query);
# 	return nextQueue;

def getNext(host_ip=None):
	global CRAWL_TYPE;
	sequences = ['renew', 'priority', 'expired', 'next'];
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	running_platform = getRunningQueue(host_ip);
	for sequence in sequences:
		query = generateNextQuery(mode=sequence);
		nextQueue = db[collection_name].find_one(query);
		if not nextQueue is None:
			return nextQueue;

def getStats(args):
	Logger.v('Queue.check: ',args);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	rows = db[collection_name].find().sort('inserted_at',-1).limit(fn.getNestedElement(args, 'limit', 10));
	keys = ['page_type', 'upid', 'priority', 'inserted_at', 'running_host','running_at', 'completed_at'];
	Logger.v('keys:',keys);
	lines = [];
	for row in rows:

		message = getQueueMessage(row); 
		data = [];
		for key in keys:
			if key in ['inserted_at','running_at', 'completed_at']:
				timestamp = fn.getNestedElement(row, key);
				if timestamp:
					timestamp = DateTime.toString(timestamp, withTimezone=True);
				data.append(timestamp);
			else:
				data.append(fn.getNestedElement(row, key));
		data.append(message);
		lines.append(data);
	fn.writeCsvFile('log/%s-queue-stats'%DateTime.toString(DateTime.now()), lines);
def getRunningQueue(host_ip):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	query = generateNextQuery({
		'running_host':host_ip, 
		'running_at':{
			'$gte': DateTime.getMinutesAgo(2) #crawl started and did not expired
		},
		'completed_at':None
	});
	runningSchedule = db[collection_name].find(query);
	# running_count = runningSchedule.count(); #get count fast
	platform = [];
	for row in runningSchedule:
		Logger.v('Queue.getRunningQueue:', row);
		platform.append(row['page_type']);
	return platform;
def getRunning(host_ip):
	running_queue = getRunningQueue(host_ip);
	return len(running_queue);

def getPending(include_incomplete=True, page_type=None, priority=None):
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	q = {'completed_at':None };
	if not include_incomplete:
		q['running_at'] = None;
	if page_type:
		q['page_type'] = page_type;
	if priority:
		q['priority'] = priority;

	queues = list(db[collection_name].find(q, {'_id':0}));
	return queues;

def create(page, extra_params={}, priority='new', duration=None, batch=False, admin=False):
	dbManager = SharedMemoryManager.getInstance();
	upid, page_type = extractParams(page);
	page = copy.deepcopy(page);
	createIndex();
	page.update({	
		'completed_at':None, 
		'running_at':None,
		'duration': (duration or fn.getNestedElement(fn.config,'CRAWL_DURATION', 12)),
		'priority':priority
	});
	if extra_params:
		Logger.v('extra:', extra_params);
		page.update(extra_params);

	if type(fn.getNestedElement(page, 'uid')) == str:#ensure uid is integer
		page['uid'] = int(fn.getNestedElement(page, 'uid'));
	elif not 'uid'in page:
		page['uid'] = 0;
	# Logger.v('Queue.Create', page);
	if isRunning(page, admin=admin): #update to refresh the status of queue
		q = {
			'upid':upid, 
			'page_type': page_type, 
			'uid': page['uid']
		};
		if admin:
			q['uid'] = page['uid'];
		dbManager.addBulkUpdate(collection_name, q, page, upsert=True, batch=batch);
		Logger.v('Update Schedule for: ', page);

	elif not isPending(page, admin=admin): #create new queue
		db = dbManager.query();
		find_queue = list(db[collection_name].find({'upid': page['upid']}));
		if find_queue:
			q = {
				'upid':upid, 
				'page_type': page_type, 
				'uid': page['uid']
			};
			if admin:
				q['uid'] = page['uid'];
			dbManager.addBulkUpdate(collection_name, q, page, upsert=True, batch=batch);
			Logger.v('Update Schedule for: ', page);
		else:
			dbManager.addBulkInsert(collection_name, page, batch=True);


	if priority == 'new':
		Mail.send('[%s]New Profile Added %s'%( DateTime.getReadableDate(DateTime.now()), fn.getNestedElement(page, 'name') ), '%s'%(page));
	return True;

def getQueueMessage(row): #intepret crawl duration as message
	running_at = fn.getNestedElement(row, 'running_at');
	completed_at = fn.getNestedElement(row, 'completed_at');
	if running_at and completed_at:
		different = completed_at-running_at;
		time_taken = math.ceil(different.total_seconds());
		message = 'complete in %s seconds'%time_taken;
	elif running_at:
		message = 'running at %s'%fn.getNestedElement(row, 'running_host');
	else:
		message = 'pending';
	return message;

def get(args):
	params, valid = Params.extract(copy.deepcopy(args), [
		{ 'src':'limit','class':int,'default':10 }, 
		{ 'src':'status','class':str, 'default':'all' }
	]);
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	rows = db[collection_name].find().sort('inserted_at',-1).limit(fn.getNestedElement(params, 'limit', 10));
	data = [];
	status = fn.getNestedElement(params,'status');
	for row in rows:
		if status == 'pending' and not row['completed_at'] == None:
			continue;
		if status == 'completed' and row['completed_at'] == None:
			continue;
		if status == 'running' and (row['running_at'] == None or not row['completed_at'] == None):
			continue;
		row['_id'] = str(row['_id']);
		row['message'] = getQueueMessage(row);
		data.append(row);
	return Params.generate(True, data);

def getPageInfo(page):
	# Logger.v('getPageInfo:',page);
	return {
		'is_pending':isPending(page),
		'is_running':isRunning(page),
	}

def getInfo(args):
	page_type = fn.getNestedElement(args, 'page_type', None);
	priority = fn.getNestedElement(args, 'priority', 'new');
	if type(priority) == list:
		priority = {'$in' : priority};
	q = {
		'completed_at': None,
		'priority': priority
	};
	if page_type:
		q['page_type'] = page_type;

	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	incomplete_queue = db[collection_name].find(q, { '_id': 0 }).sort('inserted_at',1);

	result = {
		'incomplete':[],
		'pending':[]
	};
	for queue in incomplete_queue:
		upid = fn.getNestedElement(queue, 'upid');
		if fn.getNestedElement(queue, 'running_at', None) == None:
			result['pending'].append(upid);
		else:
			result['incomplete'].append(upid);

	Logger.v('check incomplete_queue', result);
	return result;

def createIndex():
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query();
	col = db[collection_name];
	index_info = col.index_information();
	index_name = 'upid_1_page_type_1';
	index = [('upid', 1), ('page_type', 1)];
	if index_name not in index_info:
		col.create_index(index);
	return True;
