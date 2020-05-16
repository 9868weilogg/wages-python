from lib import *;
from Facebook import Post as FBPost;
from Instagram import Post as IGPost;
from HashTag import Post as HTPost;
def cache(args):
	Logger.v('CrawlPatch.cache:', args);
	params, valid = Params.extract(args, [
		{ 'src':'pid'}, 
		{ 'src':'type','target':'page_type', },
		{ 'src':'permalink', 'required':-1},
		{ 'src':'upid', 'required':-1}, 
	]);
	if not valid:
		Logger.e('Patch.cache failed, missing params');
		return None;
	page_type = fn.getNestedElement(params, 'page_type');
	if page_type == 'fb':
		result = FBPost.cache(params);
	elif page_type == 'ig':
		result =  IGPost.cache(params);
	elif page_type == 'ht':
		result =  HTPost.cache(params);	
	return { 'cache_full_picture': result };
