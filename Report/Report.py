import copy;

from lib import Logger;
from lib import fn;
from lib import DebugManager;
from lib import Params;
from lib import SharedMemoryManager;
from Report import Filter;
from Report import File;
from Report import Budget;
from Report import Stock;
from Model import Procurement as ModelProcurement;
from Model import Budget as ModelBudget;

def get(action, params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	if action == 'whitelist':
		result = getWhitelist(params);
	elif action == 'procurement':
		Logger.v('action:', action);
		# result = getResult(action, params); # XXX
		result = getResult2(action, params); # XXX
	elif action == 'budget':
		Logger.v('action:', action);
		# result = getResult(action, params); # XXX
		result = getResult2(action, params); # XXX
	elif action == 'stockcheck':
		Logger.v('action', action);
		result = Stock.run(params);
	
	Debug.end();
	Debug.show('Report.get');
	return Params.generate(True, result);

def getWhitelist(params):
	# Logger.v('Retrieve report param', params);
	# fn.show(params);
	return params;

def getResult2(action, params):
	Debug = DebugManager.DebugManager();
	Debug.start();
	Debug.trace('start')
	if action == 'procurement':
		result = ModelProcurement.get(params=params);
		# result = Budget.get(params=params);
	elif action == 'budget':
		result = ModelBudget.get(params=params);
		# result = Budget.get(params=params);
	Debug.end();
	Debug.show('Report.getResult2');
	return result;

def getReportName(params):
	action = fn.getNestedElement(params, 'action');
	report_name = action.split('_')[-1];
	return report_name;