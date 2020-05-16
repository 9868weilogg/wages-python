from Model import Facility as ModelFacility;

def get(params):
	facility_list = ModelFacility.get(params);
	return facility_list;