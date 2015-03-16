from __future__ import print_function, unicode_literals, division

from copy import deepcopy
from datetime import datetime, timedelta

from System.Threading import Timer, TimerCallback, Timeout

from SqlServer import SqlServerConnectionBase as SqlConnection

class SqlJob(object):

	def __init__(self, name, *args, **kwargs):
		self._name = name

		super(SqlJob, self).__init__(*args, **kwargs)

	name = property(lambda self: self._name)

	def __call__(self):
		pass

class ScheduleManager(object):

	def __init__(self, *args, **kwargs):
		self._safe_work_windows = []
		self._scheduled = []
		self._expired = []

		super(self, ScheduleManager).__init__(*args, **kwargs)

	def add_job(self, job):
		pass

class SqlServerMonitor(SqlConnection):


	def __init__(self, sql_instance, graphite_root=None, **kwargs):
		""" Monitors a specified sql server.  Can query db server health.
		"""
		server, backslash, instance = sql_instance.partition('\\')

		# dot separated server / instance name. safe to use in the metric path sent to graphite
		self._server_identifier = server 	
		if instance:
			self._server_identifier += ('.' + instance)

		self._queue = None
		self._metrics = {}
		self._graphite_root = graphite_root

		# self._schedule_manager = ScheduleManager()

		if 'logger_name' not in kwargs:
			kwargs['logger_name'] = self._server_identifier
		super(SqlServerMonitor, self).__init__(sql_instance, **kwargs)

		_delegate = TimerCallback(self.look_for_work)
		self._t = Timer(_delegate, None, Timeout.Infinite, Timeout.Infinite)	# instantiate timer, but do not start (started in __call__).

	name = property(lambda self: self._server_identifier)

	def __getitem__(self, metric_name):
		return self._metrics[metric_name]

	graphite_root = property(lambda self: self._metric_root, None, None
		, 'The lowest level node of the graphite path to be constructed for all graphite metrics.')
	@graphite_root.setter
	def graphite_root(self, value):
		self._metric_root = value

	def build_metric_root(self, metric_name):
		""" Returns the root path of a given metric on a sql server.  Because graphite has different retention
			policies for metrics that poll at 5, 15, or 60 seconds, this must be included in the root path.
		"""
		# interval can be -1 when disabled.
		if self[metric_name].interval_seconds < 0:
			return None

		root = self._graphite_root + '.' if self._graphite_root else ""

		root += self._server_identifier

		root += ".{}seconds".format(self[metric_name].interval_seconds)

		return root

	@SqlConnection.log_to('debug')
	def add_metric(self, metric, interval_seconds=None):
		if metric.name in self._metrics:
			self.error('''Cannot add the same metric to a server more than once.\nTo change a metric polling interval, address the attached metric directly.''')
			return False

		m = deepcopy(metric)
		if interval_seconds:
			m.interval_seconds = interval_seconds
		m.target = self
		m.next_run_time = datetime.now() + timedelta(seconds=m.interval_seconds)
		self._metrics[m.name] = m

		return self

	@SqlConnection.log_to('debug')
	def remove_metric(self, metric):
		n = metric.name
		del self._metrics[n]
		self.debug("Removed metric <<{}>>".format(n))

	def list_metrics(self):
		for k in self._metrics.keys():
			yield k

	def look_for_work(self, obj=None):
		""" Function called on timer to check for & execute scheduled jobs, metrics.
			When called from TimerCallback, two arguments are always passed.  The second is intended for a state Event.
		"""
		dt = datetime.now()

		self.check_jobs(dt)

		self.check_metrics(dt)

		return self

	def check_jobs(self, at_datetime=None):
		pass

	def check_metrics(self, at_datetime=None):
		""" Function cycles through all metrics held in the internal list of _metrics, and runs those scheduled.
		"""
		if not at_datetime:
			at_datetime = datetime.now()
		metrics = [self[m] for m in self.list_metrics() if at_datetime >= self[m].next_run_time]

		for metric in metrics:
			try:
				if metric(self._queue, root_path=self.build_metric_root(metric.name)):
					metric.last_run_time = at_datetime
			except (Exception) as e:
				self.exception(e)
				raise(e)

			metric.next_run_time = at_datetime + timedelta(seconds=metric.interval_seconds)

		return self

	def __call__(self, queue):
		""" When the SqlServerMonitor is called, the timer is started, and individual metrics will be called
			on their own individual schedules.  The results are enqueued in the queue object that is passed.
		"""
		self._queue = queue
		self.run()
		return self

	def run(self):
		self._t.Change(1000, 1000)	# wait one second, and begin calling look_for_work once a second.

	def pause(self):
		self._t.Change(Timeout.Infinite, Timeout.Infinite)

	def quit(self):
		self.__del__()

	def __del__(self):
		self.release_timer()

	def release_timer(self):
		self._t.Dispose()
