from __future__ import print_function, unicode_literals, division

from collections import namedtuple	# get_columns result

import clr
clr.AddReference('System.Data')
from System.Data.SqlClient import SqlConnection, SqlException, SqlCommand
from System import Convert, InvalidOperationException

from ApplicationBase import WindowsAppLoggingBase as Logging
from LoggingBase import format_exception

def split_mssql_object(object_name):
	db = None
	schema = None
	if '.' in object_name:
		r = object_name.split('.')
		if len(r) > 3:
			exception_message = 'A qualified object name in Microsoft SQL must conform to the standard <<optional [DatabaseName].>><<optional [SchemaName].>>[ObjectName].'
			exception_message += '\n'
			exception_message += 'The object named "{}" contains more than two dots.'.format(object_name)
			raise Exception(exception_message)
		elif len(r) == 3:
			db, schema, object_name = r
		else:
			schema, object_name = r

	# handle [DatabaseName]..[ObjectName]
	if schema is None and db is not None:
		schema = 'dbo'

	return db, schema, object_name


class SqlServerConnectionBase(Logging):

	_object_quoting_characters = '[]'

	def __init__(self, sql_instance, db='master', **kwargs):
		self._instance = sql_instance
		self._db = db

		self._exception_message = None
		self._last_rowcount = 0

		super(SqlServerConnectionBase, self).__init__(**kwargs)

		# allow for user to specify other object identifier besides []
		if 'object_quoting_characters' in kwargs:
			 q = kwargs.pop('object_quoting_characters')
			 if len(q) > 2:
			 	raise Exception("Only two characters can enclose object names.  User specified: {}".format(q))
			 if len(q) == 1:
			 	self._object_quoting_characters = q + q
			 else:
				 self._object_quoting_characters = q
		self._object_quoting_char_left = self._object_quoting_characters[0]
		self._object_quoting_char_right = self._object_quoting_characters[1]

		self._object_quotename_cache = {}
		self._quoted_db = self.quotename(self.db)

		self._sql_server_version = self.scalar_result('SELECT @@version;')
		if self.raised_exception:
			print(self.exception_message)
			raise Exception(self.exception_message)
		self.info("Successfully connected to the instance {} running {}".format(self.instance, self._sql_server_version))

	instance = property(lambda self: self._instance)

	sql_server_version = property(lambda self: self._sql_server_version)

	db = property(lambda self: self._db)
	@db.setter
	def db(self, db):
		self._db = db
		self._quoted_db = self.quotename(db)

	quoted_db = property(lambda self: self._quoted_db)

	last_rowcount = property(lambda self: self._last_rowcount)

	exception_message = property(lambda self: self._exception_message)

	@property
	def raised_exception(self):
		return True if self._exception_message else False

	def clear_exception(self):
		e = self._exception_message
		self._exception_message = None
		return e

	def is_null_or_none(self, value):
		return Convert.IsDBNull(value) or value is None

	@Logging.log_to('debug')
	def scalar_result(self, query, db=None, log_query=False):
		result = None

		connection_string = "server={};database={};Trusted_Connection=True;".format(self.instance, db if db else self.db)
		query = query.replace('\t', ' ')
		if log_query:
			self.info(query)

		try:
			with SqlConnection(connection_string) as con:
				with SqlCommand(query, con) as command:
					con.Open()
					result = command.ExecuteScalar()
					self._last_rowcount = 0 if self.is_null_or_none(result) else 1
		except (InvalidOperationException) as i:
			self._exception_message = i.Message
			self.error("The connection defined is invalid:\n{}\n\n{}".format(connection_string, self.exception_message))
			self.exception(i)

			return False
		except (SqlException) as e:
			self._exception_message = e.Message
			self.error("The query generated an exception:\n\t{}\n\n{}".format(query, self.exception_message))
			self.exception(e)

			return False

		return result

	@Logging.log_to('debug')
	def no_result(self, query, db=None, log_query=False):
		connection_string = "server={};database={};Trusted_Connection=True;".format(self.instance, db if db else self.db)
		query = query.replace('\t', ' ')
		if log_query:
			self.info(query)

		try:
			with SqlConnection(connection_string) as con:
				with SqlCommand(query, con) as command:
					con.Open()
					self._last_rowcount = command.ExecuteNonQuery()
		except (InvalidOperationException) as i:
			self._exception_message = i.Message
			self.error("The connection defined is invalid:\n{}\n\n{}".format(connection_string, self.exception_message))
			self.exception(i)

			return False
		except (SqlException) as e:
			self._exception_message = e.Message
			self.error("The query generated an exception:\n\t{}\n\n{}".format(query, self.exception_message))
			self.exception(e)

			return False

		return self.last_rowcount

	@Logging.log_to('debug')
	def query_results(self, query, db=None, log_query=False):
		""" Generator function returns the rows of data from a query run against a SqlConnection.
			If the log_query flag is set to True, the query will be written to the log as an informational message.
		"""
		connection_string = "server={};database={};Trusted_Connection=True;".format(self.instance, db if db else self.db)
		query = query.replace('\t', ' ')
		if log_query:
			self.info(query)

		try:
			with SqlConnection(connection_string) as con:
				with SqlCommand(query, con) as command:
					con.Open()
					reader = command.ExecuteReader()

					self._last_rowcount = 0
					if not reader.HasRows:
						yield None
					while reader.Read():
						self._last_rowcount += 1
						yield reader
		except (InvalidOperationException) as i:
			self._exception_message = i.Message
			self.error("The connection defined is invalid:\n{}\n\n{}".format(connection_string, self.exception_message))
			self.exception(i)

			yield False
		except (SqlException) as e:
			self._exception_message = e.Message
			self.error("The query generated an exception:\n{}\n\n{}".format(query, self.exception_message))
			self.exception(e)

			yield False

	def check_object_exists(self, object_name, object_type_code=None):
		if object_type_code:
			r = self.scalar_result(
				"SELECT CASE when object_id(N'{}', N'{}') is NULL then 0 else 1 END;".format(
					self.quotename(object_name), object_type_code
					)
				)
		r = self.scalar_result(
			"SELECT CASE when object_id(N'{}') is NULL then 0 else 1 END;".format(self.quotename(object_name))
			)

		return bool(r)

	def check_schema_exists(self, schema_name):
		return bool(
			self.scalar_result(
				"SELECT CASE when EXISTS (select * from sys.schemas where name = N'{}') then 1 else 0 END;".format(schema_name)
				)
			)

	@Logging.log_to('debug')
	def quotename(self, object_name):
		""" Takes an SQL object name, and returns the objectname as run through the TSQL function quotename.
			The function will handle splitting fully qualified objects defined by dot notation.
		"""
		if object_name in self._object_quotename_cache:
			return self._object_quotename_cache[object_name]

		def call_quotename(obj):
			if obj[0] != self._object_quoting_char_left or obj[-1] != self._object_quoting_char_right:
				obj = self.scalar_result("SELECT quotename(N'{}');".format(obj))
			return obj

		self._object_quotename_cache[object_name]  = '.'.join(
			[call_quotename(o) for o in split_mssql_object(object_name) if o is not None]
			)
		return self._object_quotename_cache[object_name]

	@Logging.log_to('debug')
	def get_columns(self, object_name):
		""" Takes an SQL object name and returns the column definitions of the object.  The object must be a view, table or 
			table value function (i.e. an object that has columns).
			get_columns will return a list of columns, with each list item a tuple containing the following values: 
				column name, datatype (SQL Server), precision and nullability flag (True if column allows nulls)
		"""
		specified_db, schema, object_name = split_mssql_object(object_name)

		query = "exec sp_columns {}".format(self.quotename(object_name))
		if schema:
			query += ", {}".format(self.quotename(schema))
		query += ";"

		Column = namedtuple('Column', 'name datatype precision is_nullable')
		return [Column(r[3],r[5],r[6],r[10]) for r in self.query_results(query, specified_db if specified_db else self.db)]

	def quotestring(self, instr):
		if isinstance(instr, basestring):
			return "'{}'".format(instr)
		return str(instr)
