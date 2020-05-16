from lib import SharedMemoryManager;
from lib import DebugManager;
from lib import fn;
from lib import Logger;
from lib import DateTime;
from lib import File;

from Model import StockIntegrity as ModelStockIntegrity;
from Model import Structure as ModelStructure;

def checkIntegrity(params):
	data = ModelStockIntegrity.check(params);
	result = ModelStructure.stockIntegrity(params=params, data=data);
	return result;