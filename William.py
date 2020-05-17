import Router;
from lib import fn;
from lib import Logger;

params = [
	##########################
	### crawl klsescreener ###
	##########################

	# {
	# 	'params': {
	# 		'action': 'crawl_bursamalaysia', # 'crawl_bursamalaysia', 'crawl_klsescreener'
	# 	}
	# },

	{
		'params': {
			'action': 'crawl_klsescreener', # 'crawl_bursamalaysia', 'crawl_klsescreener'
		}
	},

]



###########################
## Loop Test python code ##
###########################

def runTest(cases):
	for case in cases:
		params = case['params'];
		result = Router.route(params);
		Logger.v('William result', result);
		# fn.show(result);
		filename = 'william_py_result';
		fn.writeTestFile(filename, result, minified=False);
		# fn.writeJSONFile(filename='tests/{0}.json'.format(filename), data=result, minified=False);

runTest(params);


