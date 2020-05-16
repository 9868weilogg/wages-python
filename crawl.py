#!/usr/bin/python3

from Crawl import Main as Crawl;
#Crawl.createSchedule();
Crawl.start();
def application(env, start_response): #trigger crawl from api without socket
	start_response('200 OK', [('Content-Type','text/html')])
	return [b"This is dummy.py"]
