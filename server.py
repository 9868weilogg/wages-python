#! /usr/bin/python3
from lib import basic;
from lib import fn;
from lib import SharedMemoryManager;
from lib import Logger;
from lib import Mail;
import zlib;
import socket;
import traceback;
import json;
import os;
import sys;
import _thread;
import Router;

HOST = fn.config['SOCKET_HOST'];
PORT = int(fn.config['SOCKET_PORT']);
LIVE = int(fn.config['LIVE']);
MAX_FORK = 10;
def start():
	try:
		if LIVE:
			Logger.start('python-server.log');
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1);
		s.bind((HOST, PORT))
		s.listen(1);
		print('socket start %s:%s'%(HOST, PORT));
		try:
			accept_conn(s);
		except KeyboardInterrupt:
			s.close();
			print('socket close');
			sys.exit(0);
	except Exception as ex:
		print(ex);

def accept_conn(s):
	sharedManager = SharedMemoryManager.Manager();
	dbManager = SharedMemoryManager.getInstance();
	db = dbManager.query(index=False);
	dbManager.close();
	while True:
		try:
			#accept and receive socket connection
			conn, addr = s.accept();
			_thread.start_new_thread(process, (conn, addr, dbManager));
		except Exception as ex:
			print(ex);
			traceback.print_exc();

def process(conn, addr, dbManager):
	SharedMemoryManager.setInstance(dbManager); #set instance of shared memory to make all new thread sync into one memory at a time.

	print('[%s][Start]'%addr[1]);
	#get and parse input params to json
	data = conn.recv(4096);

	if not data: #stop if no data
		conn.close();
		return
	params = None;
	output = None;
	try:
		args = data.decode('utf-8','ignore');
		# basic.reloadlib(Router);
		params, method = basic.decodeHeader(args);
		# print('[%s][Process] %s'%(addr[1], params));
		result = Router.route(params);

		if(method == 'POST'):
			header = 'POST HTTP/1.1 200 OK' if result['success'] else 'HTTP/1.1 500 Python Server Error';
			output = fn.dumps("%s\n%s"%(header,str(result)));
		else:
			uncompress_data = fn.dumps(result, indent=0);
			z = zlib.compressobj(-1,zlib.DEFLATED,31);
			gzip_compressed_data = z.compress(uncompress_data) + z.flush();
			output = gzip_compressed_data;
	except Exception as ex:
		Logger.e('Server:', ex);
		traceback.print_exc();
		result = {'success':False,'uncaught':ex};
		header = 'POST HTTP/1.1 200 OK' if result['success'] else 'HTTP/1.1 500 Python Server Error';
		output = fn.dumps("%s\n%s"%(header,str()));
		# if LIVE:
		# 	Mail.send('[%s]%s:Error on server.py IP '%( DateTime.now(), basic.getCurrentIP()), '%s <br/> %s'%(params, ex));

	send(conn, output);
	conn.close();
	print('[%s][Complete] '%addr[1]);

def send(conn, output):
	if(output):
		conn.sendall(output);

start();