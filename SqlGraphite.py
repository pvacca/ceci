from __future__ import print_function, unicode_literals, division

import socket
from threading import Event

import clr
clr.AddReference('System.Collections')

from System.Collections.Concurrent import ConcurrentQueue
from System.Threading import Timer, TimerCallback, Timeout

from ApplicationBase import WindowsAppLoggingBase as LoggingBase

class SqlMonitorGraphiteRunner(LoggingBase):


	def __init__(self, **kwargs):
		self._servers = {}
		self._queue = ConcurrentQueue[dict]()

		self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)	# UDP
		self._graphite_server = None
		self._graphite_port = None
		self._is_paused = Event()
		self._echo = False
		self._silent = False

		_delegate = TimerCallback(self.send_to_graphite)
		self._send_timer = Timer(_delegate, None, Timeout.Infinite, Timeout.Infinite)

		super(SqlMonitorGraphiteRunner, self).__init__(**kwargs)
		self.info(">>>>>>>>>>>>>>>>>>LET'S!>>START!>>RUNNING!!!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

	def add_server(self, server):
		self[server.name] = server
		return self

	def __getitem__(self, server_name):
		if '\\' in server_name:
			server_name = server_name.replace('\\', '.')
		return self._servers[server_name]

	@LoggingBase.log_to('debug', log_with_params=True)
	def __setitem__(self, server_name, value):
		if '\\' in server_name:
			server_name = server_name.replace('\\', '.')
		if server_name in self._servers:
			self.error('''Cannot add the same server and instance to the monitor runner twice.  To alter metrics attached to the server, address the attached server directly.''')
		else:
			self._servers[server_name] = value

	graphite_server = property(lambda self: self._graphite_server)
	@graphite_server.setter
	def graphite_server(self, value):
		self._graphite_server = value

	graphite_port = property(lambda self: self._graphite_port)
	@graphite_port.setter
	def graphite_port(self, value):
		self._graphite_port = value

	echo = property(lambda self: self._echo, None, None
		, '''Echoes all received metrics to the screen when True. When False still prints "." to the terminal while running and "*" while sending. To suppress all output set "silent" to True.''')
	@echo.setter
	def echo(self, value):
		self._echo = bool(value)

	silent = property(lambda self: self._silent, None, None
		, '''Suppresses all terminal output.''')
	@silent.setter
	def silent(self, value):
		self._silent = bool(value)

	def __call__(self, graphite_server=None, graphite_port=None, echo=None):
		self.start(graphite_server, graphite_port, echo)

	@LoggingBase.log_to('debug', log_with_params=True)
	def start(self, graphite_server=None, graphite_port=None, echo=None):
		""" On start, the runner passes the ConcurrentQueue object to all added servers as a place to store graphite metric 
			results.  The results are then dequeued and sent to the graphite server & port by the GraphiteMonitor.

			Server & port should either be specified in this method, or assigned directly to the GraphiteMonitor object using the exposed 
			properties before the start command is issued.
		"""
		if graphite_server:
			self._graphite_server = graphite_server
		if graphite_port:
			self._graphite_port = graphite_port
		if echo:
			self._echo = echo
		for server_name in self._servers:
			# pass the collection queue to each server being monitored, and start monitoring on those servers.
			self[server_name](self._queue)

		return self.run()

	@LoggingBase.log_to('debug')
	def run(self):
		self._send_timer.Change(0, 1000)	# start immediately, 1 second interval.
		self._is_paused.clear()

		return self

	@LoggingBase.log_to('debug')
	def pause(self):
		self._is_paused.set()
		self._send_timer.Change(Timeout.Infinite, Timeout.Infinite)

	@LoggingBase.log_to('debug')
	def quit(self):
		self.__del__()

	def __del__(self):
		try:
			for server_name in self._servers.keys():
				self[server_name].release_timer()
			while self.send_to_graphite():
				pass
		except (Exception) as e:
			self.exception(e)
			raise e
		finally:
			self._socket.close()
			self._send_timer.Dispose()

	def send_to_graphite(self, item=None):
		""" This recursive function will continue to dequeue and send all enqueued items one at a time 
			over UDP to the graphite server & port. 
			If echo is ON and no items are found to send, prints a dot (".") to the screen. 
		"""
		if not item:
			if not self.silent:
				print('.', end='')
		else:
			message = "{graphite_path} {value} {timestamp}\n".format(**item)
			if not self.silent:
				print(message if self.echo else '*', end='')
			self._socket.sendto(message, (self.graphite_server, self.graphite_port))

		got_item, item = self._queue.TryDequeue()
		if got_item:
			self.send_to_graphite(item)
		return got_item


