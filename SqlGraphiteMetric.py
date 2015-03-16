from __future__ import print_function, unicode_literals, division

import time

def append_dot(instring):
	""" Function returns a string with a single dot at the end, or an empty string if passed an empty object.
		This is usefull for building dot separated nodes from arbitrary strings that may already
		terminate with a dot, or be empty or None.
	"""
	if not instring:
		return ""	# return empty string if passed None
	if instring.endswith('.'):
		return instring
	return instring + '.'

class GraphiteSqlMetric(object):

	metric_intervals = {-1, 5, 15, 60}

	def check_interval(self, interval_seconds):
		if interval_seconds not in self.metric_intervals:
			raise Exception('A metric can only poll at one of the following intervals (in seconds): {}'.format(self.metric_intervals))
		return interval_seconds

	def __init__(self, function_name, key_columns=[], path_descriptor=None, metric_name=None, metric_path_function=None, interval_seconds=60):
		""" Creates a graphite poller based on results of a SQL Server table value function.
			If key_columns are specified, they should be ordered as appropriate in the graphite path, or a formatter 
			function should be specified as "metric_path_function".

			Graphite metrics are sent as a dot separated hierarchal metric path with integer values representing
			the metric's value at a time.  The metric takes the format: "the.metric.path <<value>> <<epoch_timestamp>>".

			The metric will be named for the path_descriptor if no name is specified.  This means that if two metrics send to the same path,
			you must specify a name for at least one of them.
		"""
		# SELECT from table value functions must always be schema qualified.
		if '.' not in function_name:
			function_name = 'dbo.{}'.format(function_name)

		# calls to quotename or sp_columns will fail if function_name ends with parens (these are only required to call the function).
		if function_name.endswith('()'):
			function_name = function_name[:-2]

		self._function_name = function_name
		self._name = metric_name if metric_name \
			else path_descriptor if path_descriptor \
				else function_name

		self._interval_seconds = self.check_interval(interval_seconds)
		self._next_run_time = None
		self._last_run_time = None

		self._path_descriptor = path_descriptor
		self._quoted_function_name = None
		self._is_ready = False
		self._target = None

		self._key_columns = key_columns 	# The columns in a metric function's result set which uniquely define the data row (e.g. a database or host name, drive letter, etc.).
		self._columns = []
		self._data_columns = []	# The difference of all columns and the provided key_columns (i.e. those columns which hold graphable data).
		self._data_metric_paths = {}	# dict uses data column as a key, and the formatable metric path (when provided key column value) as a value.

		if metric_path_function:
			self.__build_result_metric_path = metric_path_function
		else:
			self.__build_result_metric_path = self.__build_generic_metric_key_path

	name = property(lambda self: self._name)

	@property
	def function_name(self):
		if self._quoted_function_name:
			return self._quoted_function_name
		return self._function_name

	interval_seconds = property(lambda self: self._interval_seconds)
	@interval_seconds.setter
	def interval_seconds(self, interval_seconds):
		if interval_seconds == -1:
			self._is_ready = False

		if self._interval_seconds == -1 and interval_seconds > 0:	# metric was disabled, but should now be enabled
			self._interval_seconds = self.check_interval(interval_seconds)
			self.try_prepare()	# sets is_ready T or F
		else:
			self._interval_seconds = self.check_interval(interval_seconds)

	next_run_time = property(lambda self: self._next_run_time, None, None
		, 'The next scheduled run date.')
	@next_run_time.setter
	def next_run_time(self, value):
		self._next_run_time = value

	last_run_time = property(lambda self: self._last_run_time, None, None
		, 'The most recently attempted run date.')
	@last_run_time.setter
	def last_run_time(self, value):
		self._last_run_time = value

	target = property(lambda self: self._target, None, None
		, 'The sql instance to which this metric is attached.')
	@target.setter
	def target(self, target):
		self._target = target

	path_descriptor = property(lambda self: self._path_descriptor, None, None
		, 'The dot separated word or words used in constructing the metric path for graphite.')

	is_ready = property(lambda self: self._is_ready)

	def check_target_exception(self):
		""" If an exception was captured on the target sql instance, it is cleared, and the function returns the exception message.
			The function otherwise returns False.
		"""
		if self.target:
			if self.target.raised_exception:
				return self.target.clear_exception()
		return False

	def try_prepare(self):
		""" In order to be ready, the GraphiteSqlMetric must be attached to a target server on which the metric function
			is present.  The function must be a table value function and any specified key columns must be found in the 
			function's column set.  All columns are stored to _columns, and all non-key columns are stored to _data_columns.

			The metric's interval cannot be set to -1, as this denotes paused sampling.
		"""
		self._is_ready = False

		if self.interval_seconds == -1:
			return False

		if not self.target:
			return False

		self._quoted_function_name = self.target.quotename(self.function_name)
		func_id = self.target.scalar_result("SELECT object_id(N'{}', N'IF');".format(self.function_name), log_query=True)

		if self.target.is_null_or_none(func_id):
			sqlmsg = self.check_target_exception()
			print( "The object {} is required, but was not found in the {} catalog, on the server: {}\n{}".format(
				self.function_name, self.target.db, self.target.instance, sqlmsg if sqlmsg else ""
				))
			return False

		self._columns = [c.name for c in self.target.get_columns(self.function_name)]

		if self.check_target_exception():
			return False

		missing_keys = [c for c in self._key_columns if c not in self._columns]
		if missing_keys:
			raise Exception(
				"The columns {} sent to the GraphiteSqlMetric as key_columns were not found in the metric function's column set.".format(missing_keys))
			return False

		self._data_columns = [c for c in self._columns if c not in self._key_columns]
		# self._data_metric_paths = dict([(c, self._build_full_metric_path(c)) for c in self._data_columns])

		self.target.info("The metric <<{}>> is ready on {}.".format(self.name, self.target.instance))
		self._is_ready = True
		return True

	def __build_generic_metric_key_path(self, result):
		""" The default metric path building function.  If any key columns are passed to the class constructor, 
			then return a dot separated string of the key column values for this result.
		"""
		if not result:
			return ""
		return '.'.join([result[c].replace(' ', '_') for c in self._key_columns])

	def build_full_metric_path(self, result, metric_measurement_name, root_path=""):
		metric_path = append_dot(root_path) + append_dot(self._path_descriptor)
		result_metric_path = metric_path + append_dot(self.__build_result_metric_path(result))
		return result_metric_path + metric_measurement_name

	def __call__(self, queue, root_path="", log_query=False):
		if not self.is_ready:
			if not self.try_prepare():
				return False

		self.target.debug("Calling <<{}>> on {}.".format(self.name, self.target.instance))

		ts = int(time.time())	# epoch time truncated to second

		for result in self.target.query_results(
				"SELECT * FROM {}();".format(self.function_name), log_query=log_query):
			if result:
				for column in self._data_columns:
					queue.Enqueue(
						dict(graphite_path=self.build_full_metric_path(result, column, root_path), value=result[column], timestamp=ts)
						)

		if self.check_target_exception():
			return False
		return True
