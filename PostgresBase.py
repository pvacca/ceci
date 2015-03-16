from __future__ import (division, print_function, unicode_literals)
import psycopg2
import psycopg2.extras

from EnterpriseAppBase import EnterpriseAppBase, format_exception

audit_column_set = (
	'insert_tz', 'last_update_tz', 'last_update_user', 'update_tz', 'update_user', 'changed'
	)

class PostgresConnectionBase(EnterpriseAppBase):

	def __init__(self, connection_settings, **kwargs):
		self._connection_settings = connection_settings

		if 'logpath' not in kwargs:
			if 'logdir' not in kwargs:
				kwargs['logpath'] = '/tmp/PostgresConnectionBase.{}.log'.format(__name__)

		self._connection_state = {'Connected':False,'MaintenanceMode':False}
		super(PostgresConnectionBase, self).__init__(**kwargs)
		self.debug("PostgresConnectionBase initialized.")

	@property
	def host(self):
		return self._connection_settings['host']

	@property
	def port(self):
		return self._connection_settings['port']

	@property
	def user(self):
		return self._connection_settings['user']

	@property
	def password(self):
		if self._connection_settings['password'] is None:
			try:
				infile = open(self._connection_settings['pwd_path'], 'rt')
				self._connection_settings['password'] = infile.read().strip()
			except (IOError) as i:
				msg = "Received '{}'' trying to open '{}'.\nCannot proceed.".format(i.strerror, i.filename)
				self.error(msg)
				raise
		return self._connection_settings['password']

	@property
	def database(self):
		return self._connection_settings['database']

	@property
	def connected(self):
		return self._connection_state['connected']

	@connected.setter
	def connected(self, value):
		self._connection_state['connected'] = value

	@property
	def maintenance_mode(self):
		return self._connection_state['MaintenanceMode']

	@maintenance_mode.setter
	def maintenance_mode(self, value):
		self.debug("set maintenance_mode {}".format(value))
		self._connection_state['MaintenanceMode'] = value

	def connect(self):
		self.debug("attempting to connect. . .")
		connection_string = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(
			self.host, self.port, self.database, self.user, self.password
			)

		try:
			self._con = psycopg2.connect(connection_string)
		except (psycopg2.Error) as e:
			self.error("{}\t{} {}".format(type(e), e.pgcode, e.pgerror))
			self.exception(e)
		else:
			self.debug("connected to {}:{}\{} as user {}".format(
				self.host, self.port, self.database, self.user
				))
			self.connected = True

		return self.connected

	def test_connection(self, print_flag=True):
		self.debug("PostgresConnectionBase test_connection")
		if not self.connected:
			return False

		cur = self._con.cursor()
		try:
			cur.execute("SELECT * FROM pg_catalog.pg_namespace as ns WHERE ns.nspname not like 'pg_%';")
			rc = cur.rowcount
			for row in cur:
				if print_flag:
					print("result from pg_catalog.pg_namespace: {}".format(str(row[0])))
				self.debug("result from pg_catalog.pg_namespace: {}".format(str(row[0])))
			self._con.commit()
			cur.execute("select current_setting('search_path');")
			x = cur.fetchone()
			if print_flag:
				print(x[0])
			self.debug(x[0])
			self._con.commit()
		except (psycopg2.Error) as e:
			self.error("{}\t{}, {} {}".format(type(e), e.cursor, e.pgcode, e.pgerror))
		finally:
			cur.close()
			del cur

		return True if rc > 0 else False

	def run_query(self, query, arguments=None, no_debug=False
		, debug_query=None, debug_arguments=None, always_list_results=False, result_dictionary=False):
		"""
		Implements exception handling and logging around "fetchone" & "fetchall" method.
		If the executed query is a SELECT and the result rowcount is > 1, the result is a list
		of tuples as returned by psycopg2.  By default, if one row is returned, the tuple is
		removed from the list.  This can be over-ridden with always_list_results.

		If the executed query is an UPDATE, returns affected rowcount on success.

		Takes parameters in the strict format defined by psycopg2; which is to say
		that all arguments (even a single argument) are passed within a tuple, and
		replaced in query only with the '%s' indicator regardless of type.  Always
		remember to call a single parameter as a tuple, e.g.
			value = 23
			r = self.run_query("select %s;", (value, ))

		If result contains one column only, that column's value is returned directly.
		If the result contains more than one column, a tuple of values is returned.

		The debug parameters are as follows: no_debug flag if query should not be
		written to debug log regardless of other debug settings.  This is used to protect
		potentially sensitive information.  A true or false message will still write 
		to the debug log to indicate query completion.
		
		Use debug_arguments alone, or debug_query and debug_arguments if the executing query 
		or arguments are extremely long and you'd like to minimize noise in the debug log.
		If the only part of the query / result you need logged is an id, or a few columns to
		facilitate later troubleshooting, these arguments can be extremely helpful.
		"""

		# replace tabs with a single space (a product of writing SQL inline in python)
		if debug_query:
			debug_query = debug_query.replace('\t', ' ')
		query = query.replace('\t', ' ')

		cur = self._con.cursor(cursor_factory=psycopg2.extras.DictCursor) if result_dictionary \
			else self._con.cursor()

		result = None
		try:
			if no_debug is False:
				self.debug(cur.mogrify(debug_query if debug_query else query
					, debug_arguments if debug_arguments else arguments
					))
		
			cur.execute(query, arguments)
			query_type = cur.statusmessage.split()[0]
			if query_type == 'UPDATE' or (query_type == 'INSERT' and 'RETURNING' not in query):
				result = cur.rowcount
				self.debug(cur.statusmessage)
			elif query_type == 'SELECT' and cur.rowcount > 1:
				result = cur.fetchall()
			else:
				if cur.rowcount < 1:
					# -1 on truncate, 0 on select 0
					result = None
				else:
					result = cur.fetchone()
					if always_list_results:
						result = [result]
					else:
						if len(result) == 1:
							result = result[0]

			self._con.commit()

			if no_debug:
				self.debug("Result withheld by No debug. Query returned a result? {}".format(True if result is not None else False))
			else:
				self.debug(result)
		except (psycopg2.Error) as e:
			self.error("{}\t{}, {} {}".format(type(e), e.cursor, e.pgcode, e.pgerror))
				# , e.diag.schema_name, e.diag.column_name, e.diag.statement_position, e.diag.message_hint
				# , e.diag.message_primary))
			self.error(format_exception(e))
			self._con.rollback()
			self._con.reset()
			raise
		except (Exception) as ex:
			self.exception(ex)
			self._con.rollback()
			self._con.reset()
			raise
		finally:
			cur.close()
			del cur

		return result

	def run_procedure(self, procedure_name, parameters=None, no_debug=False):
		self.debug("run_procedure: {} {}".format(procedure_name, parameters))
		cur = self._con.cursor()

		result = None
		try:
			result = cur.callproc(procedure_name, parameters)

			self._con.commit()

			if no_debug:
				self.debug("Result withheld by No debug. Query returned a result? {}".format(True if result is not None else False))
			else:
				self.debug(result)
		except (psycopg2.Error) as e:
			self.error("{}\t{}, {} {}".format(type(e), e.cursor, e.pgcode, e.pgerror))
			self.error(format_exception(e))
			self._con.rollback()
			self._con.reset()
			raise
		except (Exception) as ex:
			self.exception(ex)
			self._con.rollback()
			self._con.reset()
			raise
		finally:
			cur.close()
			del cur

		return result

	def get_object_relid(self, object, schema='public'):
		select = """SELECT c.oid FROM pg_catalog.pg_class as c
		LEFT JOIN pg_catalog.pg_namespace as n
		  ON c.relnamespace = n.oid
		WHERE c.relname = %(object)s
		  AND n.nspname = %(schema)s;
		"""
		return self.run_query(select, {'object': object, 'schema': schema}, no_debug=True)


	def get_columns(self, table, schema='public'):
		relid = self.get_object_relid(object=table, schema=schema)

		select = """SELECT a.attnum as column_order
			, a.attname as column_name
			, pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type
			, not a.attnotnull as column_nullable
			, (SELECT c.collname FROM pg_catalog.pg_collation as c, pg_catalog.pg_type as t
		   		WHERE c.oid = a.attcollation AND t.oid = a.atttypid 
		   		  AND a.attcollation <> t.typcollation
		   		) AS column_collation
		FROM pg_catalog.pg_attribute as a
		WHERE a.attrelid = %(relid)s AND NOT a.attisdropped AND a.attnum > 0
		ORDER BY 1;"""

		return self.run_query(select, {'relid': relid}, result_dictionary=True)

	def get_column_names(self, table, schema='public', no_audit_columns=True):
		relid = self.get_object_relid(object=table, schema=schema)

		select = """SELECT a.attname as column_name
		FROM pg_catalog.pg_attribute as a
		WHERE a.attrelid = %(relid)s AND NOT a.attisdropped AND a.attnum > 0"""

		if no_audit_columns:
			select += " AND a.attname NOT IN %(audit_column_set)s"

		select += " ORDER BY a.attnum;"

		result = self.run_query(select, {'relid': relid, 'audit_column_set': audit_column_set})

		return set([r[0] for r in result])

	def __del__(self):
		self._con.close()
		self.debug("delete self: connection closed")

	def build_simple_merge_insert(self, table, values):
		str_columns = ','.join(values.keys())
		str_values = ','.join(["%({})s".format(k) for k in values.keys()])
		insert = "INSERT INTO {table}({str_columns}) VALUES({str_values});"

		return insert.format(table=table, str_columns=str_columns, str_values=str_values)

	def build_simple_merge_where_key(self, key):
		if isinstance(key, basestring):
			return "x.{key}=%({key})s".format(key=key)

		return "({})".format(" AND ".join("x.{n}=%({n})s".format(n=k) for k in key))

	def build_simple_merge_where_key_value(self, key, values):
		if isinstance(key, basestring):
			return "x.{key}=%({key})s".format(key=key), {key: values[key]}

		kd = dict([(k, values[k]) for k in key])
		strs = ["x.{n}=%({n})s".format(n=k) for k in key]

		return "({})".format(" AND ".join(strs)), kd

	def build_simple_merge_check_values(self, table, key, values):
		str_check_values = " AND ".join(["x.{n} = %({n})s".format(n=k) for k in values.keys()])
		str_where_key = self.build_simple_merge_where_key(key)

		exists = "SELECT EXISTS ("
		exists += "select * from {table} as x WHERE {str_where_key} and NOT ({str_check_values})"
		exists += ");"

		return exists.format(table=table, str_where_key=str_where_key, str_check_values=str_check_values)

	def build_simple_merge_update(self, table, key, values, keep_latest, keep_earliest):
		str_set_clause = ",".join(["{n}=%({n})s".format(n=k) for k in values.keys() \
			if k not in keep_latest + keep_earliest])

		if keep_latest:
			if not hasattr(keep_latest, '__iter__'):
				keep_latest = [keep_latest]

			latest = ",".join(["{n}=CASE WHEN %({n})s > coalesce(x.{n}, '-infinity') THEN %({n})s ELSE x.{n} END".format(n=k) for k in keep_latest])
			str_set_clause = ",".join([str_set_clause, latest])

		if keep_earliest:
			if not hasattr(keep_earliest, '__iter__'):
				keep_earliest = [keep_earliest]

			earliest = ",".join(["{n}=CASE WHEN %({n})s < coalesce(x.{n}, 'infinity') THEN %({n})s ELSE x.{n} END".format(n=k) for k in keep_earliest])
			str_set_clause = ",".join([str_set_clause, earliest])

		str_where_key = self.build_simple_merge_where_key(key)

		update = "UPDATE {table} as x SET {str_set_clause} WHERE {str_where_key};"

		return update.format(table=table, str_where_key=str_where_key, str_set_clause=str_set_clause)


	# todo: split merge functionality to PGMergeExtension class
	# todo: handle sending None value to merge into dest
	# DONE: handle keeping dates when earlier / later than existing value on merge.  yeah, that wasn't so hard!
	def simple_merge(self, schema, table, key, values, keep_latest=[], keep_earliest=[]):
		self.debug("simple_merge({}, {}, {}, {})".format(schema, table, key, len(values)))

		if schema:
			self.run_query("SET SCHEMA %s;", (schema, ))

		str_where_key, key_dict = self.build_simple_merge_where_key_value(key, values)
		chk = "SELECT EXISTS (select * FROM {table} as x WHERE {str_where_key});"

		# check if row exists
		if self.run_query(chk.format(table=table, str_where_key=str_where_key), key_dict):
			# check if all values match existing
			if not self.run_query(self.build_simple_merge_check_values(table, key, values), values):
				#update
				self.run_query(self.build_simple_merge_update(table, key, values, keep_latest, keep_earliest), values, no_debug=True)
		else:
			# row does not exist, new insert
			self.run_query(self.build_simple_merge_insert(table, values), values, no_debug=True)

		return self
