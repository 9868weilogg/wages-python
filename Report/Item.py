import asyncio;
import re;
from bson import regex
from lib import fn, Logger, File, SharedMemoryManager, Params;
from Model import Item as ModelItem;

def find(params):
	group_by = fn.getNestedElement(params, 'group_by', None);
	data = ModelItem.search(params);
	# Logger.v(data);
	result = ModelItem.processItemList(data, group_by);
	return Params.generate(True, result);