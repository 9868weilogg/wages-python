import Router;
from lib import fn;
from lib import Logger;

params = [
	{
		'params': {
			'action': 'upload_stock', # 'upload_stock', 'upload_facility'
			'id': 1,
			'date': '2020-04-17',
			'group': 'stock',
			'data_part': 'desh',
			# 'path': '/home/vagrant/adqlo/phis-api/storage/app/upload/stock/2020-03-29.xlsx', # local smaller filepath
			'path': '/home/vagrant/Code/phis-python/upload/desh.csv', # local larger filepath
			'callback_url': None,
			
		}
	},
	{
		'params': {
			'action': 'upload_stock', # 'upload_stock', 'upload_facility'
			'id': 2,
			'date': '2020-04-17',
			'group': 'stock',
			'data_part': 'desk',
			# 'path': '/home/vagrant/adqlo/phis-api/storage/app/upload/stock/2020-03-29.xlsx', # local smaller filepath
			'path': '/home/vagrant/Code/phis-python/upload/desk.csv', # local larger filepath
			'callback_url': None,
			
		}
	}
]



###########################
## Loop Test python code ##
###########################

def runTest(cases):
	for case in cases:
		params = case['params'];
		result = Router.route(params);
		Logger.v('Daniel result', result);
		# fn.show(result);
		filename = 'daniel_py_result';
		fn.writeTestFile(filename, result, minified=False);
		# fn.writeJSONFile(filename='tests/{0}.json'.format(filename), data=result, minified=False);

def extractSearch():
	from lib import File, Logger, DebugManager;
	from Report import Item, Stock;
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start');

	# filename = '2020-03-30.xlsx';
	filename = 'upload/desk.csv'
	# fn.ensureDirectory('tests');
	data = Item.upload(filename);

	filename = 'upload/desh.csv'
	# fn.ensureDirectory('tests');
	data = Item.upload(filename, new = False);
	# Stock.upload({
	# 	'action': 'upload_stock',
	# 	'id': 1,
	# 	'date': '2020-03-30',
	# 	'path': filename, # staging filepath
	# });
	# Item.save(data);
	# 

	result = Item.find({
		'search_text': 'oxygen',
		# 'item_code': 'Y1690580002.00,Y1690130024.00'
		# 'group_by' : 'sub_group_desc'
	});
	fn.show(result);
	# for d in result['data']:
	# 	Logger.v(d);
	# File.writeJSONFile('search.json',data);
	Debug.trace('end');
	Debug.end();
	Debug.show('Daniel.py');



##############            TEST         #############
runTest(params);
# extractSearch();