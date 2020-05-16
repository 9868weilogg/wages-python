#!/usr/bin/python3

from Crawl import Main as Crawl;
from lib import fn;
from lib import DateTime;

try:
	start_time = DateTime.now();
	fn.writeTestFile(filename='cron_start_{0}'.format(DateTime.now()), data='successful cron tab, cron time:{0}'.format(start_time));
	Crawl.run({
		'process': ['schedule', 'crawl', 'update'], # 'schedule', 'crawl', 'update'
		'schedule_params': {
			'start_crawl': False,
			'check_empty': True,
		},
		'interval': 1, # months between start date - end date 
		'keys': {
			'option': ['state', 'ptj', 'facility', 'facility_type'],
			'report': ['budget', 'procurement'], # 'budget', 'procurement'
		},
		'filter': {
			'facility_code': False,
		},
	})
	end_time = DateTime.now();
	print('end_time', end_time)
	fn.writeTestFile(filename='cron_end_{0}'.format(end_time), data='successful cron tab, cron time:{0}, used time:{1}'.format(end_time, end_time - start_time));

except Exception as ex:
	fn.writeTestFile(filename='cron_error_{0}'.format(DateTime.now()), data='-------- error: --------\n {0}'.format(ex));