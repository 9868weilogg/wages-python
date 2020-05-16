from lib import fn;
from lib import Logger;
import json;
import os;

def readJson(filename):
	data = [];
	try:
		with open(filename) as f:
			data = json.load(f);
		Logger.v('Reading:', filename);
	except Exception as e:
		Logger.e(e);

	return data;

def getLatestFile(directory, extension='.json'):
	directories = os.listdir(directory);
	# Logger.v('directories', directories);
	filtered_file = list(filter(lambda x: x.endswith(extension), directories));
	# Logger.v('filtered_file', filtered_file);
	sorted_file = sorted(filtered_file, key=lambda x: x, reverse=True);
	# Logger.v('sorted_file', sorted_file);
	if sorted_file:
		latest_file = sorted_file[0];
	return '{0}/{1}'.format(directory, latest_file);

def readLatestFile(directory, extension='.json'):
	filename = getLatestFile(directory=directory, extension='.json');
	data = readJson(filename);
	return data;