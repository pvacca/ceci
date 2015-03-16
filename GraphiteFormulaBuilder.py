from __future__ import division

from datetime import datetime, timedelta
import urllib2
from urllib2 import HTTPError

from System.Threading import Timer, TimerCallback, Timeout

from Metrics import *
from SqlServer import SqlServerConnectionBase as SqlConnection

url_safe_encoding = { 
	  ' ': '%20'
	, '!': '%21'
	, '"': '%22'
	, '#': '%23'
	, '$': '%24'
	, '%': '%25'
	, '&': '%26'
	, "'": '%27'
	, '(': '%28'
	, ')': '%29'
	, '*': '%2A'
	, '+': '%2B'
	, ',': '%2C'
	, '-': '%2D'
	# , '.': '%2E'
	# , '/': '%2F'
	}

def encode_url(url):
	def enc(ch):
		try:
			return url_safe_encoding[ch]
		except (KeyError):
			return ch

	return ''.join(map(enc, url))


class GraphiteMetricName(object):


	def __init__(self, server_name, metric_name, measurement_name, **metric_keys):
		self._server = server_name.replace('\\', '.')	#instance name is dot separated in graphite runner
		self._metric = metric_name
		self._measurement = measurement_name
		self._keys = metric_keys

		self._full_metric_path = None
		self._granularity = None

	def __str__(self):
		if not self._full_metric_path:
			return "{}({}, {}, {}, {})".format(self.__class__, self._server, self._metric, self._measurement, self._keys)
		return self._full_metric_path

	granularity = property(lambda self: self._granularity)
	@granularity.setter
	def granularity(self, value):
		self._granularity = value

	full_metric_path = property(lambda self: self._full_metric_path)
	@full_metric_path.setter
	def full_metric_path(self, value):
		self._full_metric_path = value

	def __call__(self, graphite):
		self._full_metric_path = graphite[self._server][self._metric].build_full_metric_path(
			self._keys
			, self._measurement
			, graphite[self._server].build_metric_root(self._metric)
			)
		self._granularity = graphite[self._server][self._metric].interval_seconds
		return self


class GraphiteFormulaBuilder(object):


	def __init__(self, formula_name):
		self._name = formula_name
		self._metrics = []
		self._formula = None
		self._formula_leader = None

	name = property(lambda self: self._name)

	def __repr__(self):
		return """{}('{}')""".format(self.__class__, self.name)

	def __str__(self):
		return self.emit_formula()

	formula = property(lambda self: self._formula)
	@formula.setter
	def formula(self, value):
		self._formula = value

	def __str__(self):
		return """'{}': {}""".format(self.name, self.formula if self.formula else "")

	def diff_timeshift(self, metric):
		# params = {'metric': metric.full_metric_path, 'granularity': metric.granularity}
		# return """diffSeries(keepLastValue({metric}), timeShift(keepLastValue({metric}), "{granularity}s"))""".format(**params)
		return """derivative({})""".format(metric.full_metric_path)

	def add_series(self, metric_series, diff_timeshift=False):
		self._metrics.append( (metric_series, diff_timeshift) )
		return self

	def build_formula(self, metric, graphite):
		if metric[1]:
			formula = self.diff_timeshift(metric[0](graphite))
		else:
			formula = metric[0](graphite).full_metric_path
			
		return formula

	def __call__(self, graphite):
		self._formula = ','.join([self.build_formula(metric, graphite) for metric in self._metrics])
		return self

	def divide_series(self, metric_series, diff_timeshift=False):
		self._metrics.append(  (metric_series, diff_timeshift) )
		self._formula_leader = "divideSeries"
		return self

	def emit_formula(self):
		if self._formula_leader:
			formula = "{}({})".format(self._formula_leader, self._formula)
		else:
			formula = self._formula
		formula = """alias(keepLastValue({}), "{}")""".format(formula, self.name)
		return encode_url(formula)

class GraphiteSqlPersist(SqlConnection):

	_sql_insert_batch_size = 1000

	def __init__(self, name, target_sql_instance, target_db, target_schema='metrics', graphite_server=None, **kwargs):
		self._name = name
		self._formulas = {}
		self._formula_urls = {}

		self._batch_id = None

		super(GraphiteSqlPersist, self).__init__(target_sql_instance, target_db, **kwargs)

		self._target_schema = target_schema
		self.__check_target_for_tables()

		self._graphite = None
		self._graphite_server = graphite_server

		self._running = False
		self._start_dt = None
		self._cutoff_dt = None
		self._next_export_dt = None
		self._last_export_dt = None

		self._duration_seconds = self.__check_for_duration(kwargs)
		self._export_interval_seconds = self.__check_for_export_interval(kwargs)
		if 'sql_insert_batch_size' in kwargs:
			self._sql_insert_batch_size = kwargs.pop('sql_insert_batch_size')

		_delegate = TimerCallback(self.look_for_work)
		self._t = Timer(_delegate, None, Timeout.Infinite, Timeout.Infinite)	# instantiate timer, but do not start (started in self.run()).

		self.info('---------------------------- HERE I AM (rock you like a hurricane) -----------------------------------')

	def __check_for_duration(self, kwargs):
		_duration_seconds = 0 if 'duration_seconds' not in kwargs \
			else kwargs.pop('duration_seconds')

		if 'duration_minutes' in kwargs:
			_duration_seconds += (timedelta(minutes=kwargs.pop('duration_minutes'))).total_seconds()

		if 'duration_hours' in kwargs:
			_duration_seconds += (timedelta(hours=kwargs.pop('duration_hours'))).total_seconds()

		if _duration_seconds == 0:
			_duration_seconds = 60
		return _duration_seconds

	def __check_for_export_interval(self, kwargs):
		_export_interval_seconds = 0 if 'export_interval_seconds' not in kwargs \
			else kwargs.pop('export_interval_seconds')

		if 'export_interval_minutes' in kwargs:
			_export_interval_seconds += (timedelta(minutes=kwargs.pop('export_interval_minutes'))).total_seconds()

		if 'export_interval_hours' in kwargs:
			_export_interval_seconds += (timedelta(hours=kwargs.pop('export_interval_hours'))).total_seconds()

		return _export_interval_seconds

	name = property(lambda self: self._name)

	target_schema = property(lambda self: self._target_schema)

	history_table = property(lambda self: self.target_schema + '.History')

	formula_table = property(lambda self: self.target_schema + '.Formulas')

	batch_table = property(lambda self: self.target_schema + '.BatchRuns')

	duration_seconds = property(lambda self: self._duration_seconds)

	export_interval_seconds = property(lambda self: self._export_interval_seconds)

	batch_id = property(lambda self: self._batch_id)

	start_dt = property(lambda self: self._start_dt)

	cutoff_dt = property(lambda self: self._cutoff_dt)

	last_export_dt = property(lambda self: self._last_export_dt)

	next_export_dt = property(lambda self: self._next_export_dt)
	@next_export_dt.setter
	def next_export_dt(self, value):
		self._next_export_dt = value

	graphite_server = property(lambda self: self._graphite_server)
	@graphite_server.setter
	def graphite_server(self, value):
		self._graphite_server = value

	is_running = property(lambda self: self._running)

	def __check_target_for_tables(self):
		for table in [self.history_table, self.formula_table, self.batch_table]:
			if not self.check_object_exists(table):
				raise Exception('The table "{}" does not exist.'.format(table))

	# 	if not self.check_schema_exists(self.target_schema):
	# 		self.no_result("CREATE SCHEMA {};".format(self.quotename(self.target_schema)))
	# 	self.no_result("CREATE TABLE {}();")

	@SqlConnection.log_to('debug', log_with_params=True)
	def add_formula(self, formula):
		if formula.name in self._formulas:
			self.error('Cannot add the same formula twice. A formula with the name "{}" already exists.'.format(formula.name))
		self._formulas[formula.name] = formula
		return self

	@SqlConnection.log_to('debug', log_with_params=True)
	def remove_formula(self, formula):
		if formula.name in self._formulas:
			del self._formulas[formula.name]

	def mark_batch_start(self):
		return self.scalar_result(
			"""INSERT INTO {}(BatchName) OUTPUT INSERTED.BatchID VALUES('{}');""".format(self.batch_table, self.name))

	def __call__(self, graphite=None):
		self.start(graphite)

	@SqlConnection.log_to('debug')
	def start(self, graphite=None):
		if graphite:
			self._graphite = graphite
			self.graphite_server = graphite.graphite_server

		self._start_dt = datetime.now()
		self._cutoff_dt = self.start_dt + timedelta(seconds=self.duration_seconds)
		self.next_export_dt = self.start_dt + timedelta(seconds=self.export_interval_seconds)

		self._batch_id = self.mark_batch_start()
		self.run()

	@SqlConnection.log_to('debug')
	def run(self):
		self._t.Change(0, 5000)	# begin calling look_for_work every five seconds.
		self._running = True

	@SqlConnection.log_to('debug')
	def pause(self):
		self._t.Change(Timeout.Infinite, Timeout.Infinite)
		self._running = False

	@SqlConnection.log_to('debug')
	def quit(self):
		print('You know I want to quit you, baby.')
		self.__del__()

	def __del__(self):
		self._running = False
		self.release_timer()
		print('But a quitter never wins.')

	@SqlConnection.log_to('debug')
	def release_timer(self):
		self._t.Dispose()

	def look_for_work(self, obj=None):
		dt = datetime.now()
		if dt > self.next_export_dt or dt > self.cutoff_dt:
			self._last_export_dt = dt
			self.next_export_dt = dt + timedelta(seconds=self.export_interval_seconds)
			try:
				self.export_graphite()
			except (Exception) as e:
				print("FAILED to export.")
				self.error("FAILED to export.")
				self.exception(e)
				self.quit()

			if dt > self.cutoff_dt:
				self.info('SUCCESS Reached cutoff date.')
				self.quit()

	@SqlConnection.log_to('debug')
	def export_graphite(self):
		for formula_name in self._formulas.keys():
			formula_id, url = self.get_url(formula_name)
			self.graphite_results(formula_id, url)

	@SqlConnection.log_to('debug')
	def get_url(self, formula_name):
		if formula_name in self._formula_urls:
			return self._formula_urls[formula_name]

		url_params = {
			'graphite_server': self.graphite_server
			, 'encoded_formula': self._formulas[formula_name](self._graphite).emit_formula()
			, 'graph_minutes': ((self.export_interval_seconds*2) // 60) + 1
			}
		url = "http://{graphite_server}/render?target={encoded_formula}&from=-{graph_minutes}minutes&format=csv".format(**url_params)
		formula_id = self.scalar_result(
			"SELECT FormulaID from {} where FormulaName = N'{}';".format(self.formula_table, formula_name)
			)
		if self.is_null_or_none(formula_id):
			formula_id = self.scalar_result(
				"INSERT INTO {}(FormulaName, FormulaURL) OUTPUT INSERTED.FormulaID VALUES(N'{}', N'{}');".format(
					self.formula_table, formula_name, url
					)
				)

		self._formula_urls[formula_name] = (formula_id, url)
		return self._formula_urls[formula_name]

	@SqlConnection.log_to('debug')
	def graphite_results(self, formula_id, url):
		self.debug(url)
		try:
			results = urllib2.urlopen(url)
		except (HTTPError) as e:
			self.error(url)
			raise e

		bundle_results = []
		i = 0

		line = results.readline()

		while line:
			m = line.strip().split(',')
			if m[-1]:
				seconds_since_batch_start = (datetime.strptime(m[1], "%Y-%m-%d %H:%M:%S") - self.start_dt).total_seconds()
				bundle_results.append( (formula_id, m[1], m[2], self.batch_id, seconds_since_batch_start, ) )
				
				i += 1
				if i >= self._sql_insert_batch_size:
					self.insert_results(bundle_results)
					bundle_results = []
					i = 0
			line = results.readline()

		if bundle_results:
			self.insert_results(bundle_results)

	@SqlConnection.log_to('debug')
	def insert_results(self, results):
		# params = {'history_table': self.history_table}

		query = r"""CREATE TABLE #t(formula_id int, measure_date datetime, value float, batch_id int, seconds_since_batch_start int);
			INSERT INTO #t VALUES"""

		query += ','.join(["({})".format(','.join([self.quotestring(r) for r in result])) for result in results])

		query += "; "

		query += r"""INSERT INTO {history_table}_overflow(FormulaID, MeasureDate, Value, BatchID)
			SELECT formula_id, measure_date, value, batch_id from #t as src
			where NOT EXISTS (select * from {history_table}_overflow where FormulaID=src.formula_id and MeasureDate=src.measure_date and BatchID=src.batch_id)
			  and EXISTS (select * from {history_table} where FormulaID=src.formula_id and MeasureDate=src.measure_date and BatchID=src.batch_id and Value <> src.value)
		;""".format(history_table=self.history_table)

		query += " "

		query += r"""INSERT INTO {history_table}(FormulaID, MeasureDate, Value, BatchID, SecondsSinceBatchStart)
			SELECT formula_id, measure_date, value, batch_id, seconds_since_batch_start from #t as src
			where NOT EXISTS (select * from {history_table} where FormulaID=src.formula_id and MeasureDate=src.measure_date and BatchID=src.batch_id)
		;""".format(history_table=self.history_table)
		
		rc = self.no_result(query, log_query=False)
		self.debug("INSERTED {} rows.".format(rc))
