from lib import SharedMemoryManager;
from lib import basic;
from lib import db;
from lib import fn;
from lib import DateTime;
from lib import Logger;
from lib import Cache;

from Crawl import Main as Crawl;
from Report import Report;
from Report import Filter;
from Report import Stock as ReportStock;
from Report import StockIssue as ReportStockIssue;
from Report import Item as ReportItem;
from Report import Facility as ReportFacility;
from Report import StockIntegrity as ReportStockIntegrity;
from Report import StockIssueIntegrity as ReportStockIssueIntegrity;
from Model import Facility as ModelFacility;
from Model import Stock as ModelStock;
from Model import StockIssue as ModelStockIssue;

def route(params):
	result = {'success':False};
	action = params['action'];
	act = action.split('_');

	### Called by API
	Logger.v('route:',params);
	if act[0] == 'test':
		if act[1] == 'whitelist':
			result = Report.get(act[1], params);
	if act[0] == 'generate':
		if act[1] == 'dropdownlist':
			result = Filter.getDropdownList(params);
		if act[1] == 'updatereport':
			result = Crawl.updateReportData(params);
		if act[1] == 'budget':
			result = Report.get(act[1], params);
		if act[1] == 'procurement':
			result = Report.get(act[1], params);
	if act[0] == 'get':
		if act[1] == 'stock':
			if act[2] == 'check':
				result = Report.get('{0}{1}'.format(act[1], act[2]), params);
			if act[2] == 'integrity':
				result = ReportStockIntegrity.checkIntegrity(params);
			if act[2] == 'backdate':
				result = ReportStock.getBackdateList(params);
		if act[1] == 'stock-issue':
			if act[2] == 'integrity':
				result = ReportStockIssueIntegrity.check(params);
			if act[2] == 'options':
				result = ReportStockIssue.getOption(params);
			if act[2] == 'check':
				result = ReportStockIssue.check(params);
		if act[1] == 'facility':
			result = ReportFacility.get(params);
	if act[0] == 'crawl':
		if act[1] == 'main':
			result = Crawl.run(params);
		if act[1] == 'start':
			result = Crawl.start(params);
		if act[1] == 'schedule':
			result = Crawl.prepareSchedule(params);
	if act[0] == 'upload':
		if act[1] == 'stock':
			result = ModelStock.upload(params);
		if act[1] == 'facility':
			result = ModelFacility.upload(params);
		if act[1] == 'stock-issue':
			result = ModelStockIssue.upload(params);
	if act[0] == 'search':
		if act[1] == 'stock': ## search_stock
			result = ReportItem.find(params);
	# fn.show(result);
	return result;