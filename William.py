import Router;
from lib import fn;
from lib import Logger;

params = [
	##########################
	### testing white list ###
	##########################

	# {
	# 	'params': {
	# 		'action': 'test_whitelist',
	# 		'duration': ['2019-06-01', '2019-08-01'],
	# 		'state': ['selangor'],
	# 		'ptj': ['0123'],
	# 		'facility': ['hospital kl'],
	# 		'facility_type': ['hospital'],
	# 	}
	# },

	#################################################
	### create schedule, crawl, update to mongodb ###
	#################################################

	# run crawl_schedule to create schedule first
	# run crawl_start to generate demo data/ crawl live API and store to local
	# run action='generate_updatereport'

	# {
	# 	'params': {
	# 		'action': 'crawl_main', # 'crawl_start', 'crawl_schedule', 'generate_updatereport', 'crawl_main'
	# 		# 'duration': ['2019-06-01', '2019-08-01'],
	# 		'process': ['schedule', 'crawl', 'update'], # 'schedule', 'crawl', 'update'
	# 		'schedule_params': {
	# 			# 'today': '2020-03-24',
	# 			'start_crawl': True,
	# 			'check_empty': True,
	# 		},
	# 		'interval': 1, # months between start date - end date 
	# 		'keys': {
	# 			'option': ['state', 'ptj', 'facility', 'facility_type'],
	# 			# 'report': ['procurement'], 
	# 			'report': ['budget', 'procurement'], # 'budget', 'procurement'
	# 		},
	# 		'filter': {
	# 			'facility_code': False,
	# 		},
	# 	} 
	# },

	####################################
	### generate filter dropdownlist ###
	####################################

	# {
	# 	'params': {
	# 		'action': 'generate_dropdownlist',
	# 	}
	# },

	##########################
	### update report data ###
	##########################

	# {
	# 	'params': {
	# 		'action': 'generate_updatereport',
	# 		'keys': {
	# 			'option': ['state', 'ptj', 'facility', 'facility_type'],
	# 			'report': ['procurement'], # 'budget', 'procurement'
	# 			# 'report': ['budget', 'procurement'], # 'budget', 'procurement'
	# 		},
	# 		'filter': {
	# 			'facility_code': False,
	# 		},
	# 	}
	# },

	#######################
	### generate report ###
	#######################

	# {
	# 	'params': {
	# 		'action': 'generate_budget', # 'generate_budget'. 'generate_procurement'
	# 		'filter': {
	# 			'year': '2020',
	# 			'months': '2', # 1,2,3
	# 			'ptj': '', # 153401,140801
	# 			'state': '', # 08,05
	# 			'facility': '', # 21-05060156,21-05050020
	# 			'facility_type': '', # hos,kd,kk
	# 			'budget_type': '', # db
	# 			'procurement_type': '',
	# 			# 'duration': 'yearly',
	# 			'min_purchase_amount': 1000,
	# 		},
	# 	}
	# },

	###########################
	### generate stock check ###
	############################
	{
		'params': {
			'action': 'get_stock-issue_check', # 'get_stock_check', 'get_stock-issue_check'
			# 'item_codes': [
			# 	# "P1520070002.00",
			# 	# "Y1690010002.00",
			# 	# "01.0606.02",
			# 	# "01.0804.02",
			# 	# "01.3666.03",
			# 	# "14.0000.01",
			# 	"13.3803.03",
			# ], # '01.0413.02' , '01.0606.02' ,  '01.0804.02' , '01.3666.03'
			'filter':{
				# 'quantity': [{'id':'gte', 'value': 100}, {'id': 'lte', 'value': 500}],
			},
			'group_by':[
				# {'id':'state','value':'kelantan'}, # wp labuan , kelantan, johor, sarawak
				# {'id':'facility','value':'11-13220638'}, # optional, 21-03100318, 11-01010011, 11-13220638
				# {'id':'requester','value':'fpl'}, # optional, example: mkfs, fanek, ccu, fpl
				# {'id':'requester_unit','value':'mkfs'}, # optional, example: mkfs, fanek, ccu # change according to api (requester)
				# {'id':'drug','value':'13.3803.03'}, # optional, example: 25.1202.04 , 25.1202.05, 13.3803.03
			],
			# 'date': '2020-04-06',
			# 'facility_group': ['clinic'], # 'hospital', 'clinic', 'pkd',
			# 'export': 'excel', # 'excel', 'pdf'

			#########  stock issue
			'drug_nondrug_code': [
				'n02aa01183p3001xx'
			],
			'state': 'sarawak',
			'start_month': '2020-01',
			'number_of_month': 4,
			'requester_group': 'all', # "substore level","pharmacy store","unit/ward"
			'issue_type': 'all', # "own consumption","external","internal"
		}
	}

	##############################
	### upload stock/ facility ###
	##############################
	# {
	# 	'params': {
	# 		'action': 'upload_stock-issue', # 'upload_stock', 'upload_facility', 'upload_stock-issue'
	# 		'id': 1,
	# 		'date': '2020-05-05',
	# 		'group': 'stock_issue',
	# 		'data_part': 'deik',
	# 		# 'path': '/home/vagrant/adqlo/phis-api/storage/app/upload/stock/desk_2020-04-29.csv', # local filepath
	# 		'path': '/home/vagrant/adqlo/phis-api/storage/app/upload/stock_issue/deik_28042020.csv', # local filepath
	# 		# 'path': '/var/www/phis-api/storage/files/upload/stock/desh_2020-04-10.csv', # staging filepath
			
	# 	}
	# }

	#####################################################
	### stock data integrity                          ###
	### stock issue data integrity                    ###
	### stock data backdate list                      ###
	### facility list                                 ###
	### stock issue landing option list               ###
	#####################################################
	# {
	# 	'params': {
	# 		# 'action': 'get_stock_integrity',
	# 		# 'action': 'get_stock-issue_integrity',
	# 		# 'action': 'get_stock_backdate',
	# 		# 'action': 'get_facility',
	# 		'action': 'get_stock-issue_options',
	# 	}
	# }
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


