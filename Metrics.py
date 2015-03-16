from SqlGraphiteMetric import GraphiteSqlMetric

# interval_seconds can always be over-ridden when the metric is added to a server.

blocks_waits = GraphiteSqlMetric('metrics.get_waiting_tasks'
	, path_descriptor='waits', metric_name='blocks_waits'
	, interval_seconds=5
	)
wait_stats = GraphiteSqlMetric('metrics.get_wait_stats'
	, path_descriptor='waits.statistics', metric_name='wait_stats'
	, interval_seconds=5
	)
async_network_waits = GraphiteSqlMetric('metrics.get_async_network_waits'
	, path_descriptor='waits.async_waits.by_host', metric_name='async_network_waits'
	, key_columns=['host']
	, interval_seconds=5
	)
sql_server_statistics = GraphiteSqlMetric('metrics.get_sql_server_statistics'
	, path_descriptor='statistics', metric_name='sql_server_statistics'
	, interval_seconds=15
	)
database_statistics = GraphiteSqlMetric('metrics.get_database_statistics'
	, path_descriptor='statistics.by_database', metric_name='database_statistics'
	, key_columns=['database_name']
	, interval_seconds=15
	)
connections = GraphiteSqlMetric('metrics.get_connections'
	, path_descriptor='statistics.connections', metric_name='connections'
	, interval_seconds=15
	)
clr_execution = GraphiteSqlMetric('metrics.get_clr_execution_time'
	, path_descriptor='statistics', metric_name='clr_execution'
	, interval_seconds=15
	)
lock_stats = GraphiteSqlMetric('metrics.get_resource_locks'
	, path_descriptor='statistics.resource_locks', metric_name='lock_stats'
	, key_columns=['waiting_resource']
	, interval_seconds=60
	)
latch_wait_stats = GraphiteSqlMetric('metrics.get_latch_waits'
	, path_descriptor='statistics', metric_name='latch_wait_stats'
	, interval_seconds=60
	)
memory = GraphiteSqlMetric('metrics.get_memory_manager'
	, path_descriptor='memory', metric_name='memory'
	, interval_seconds=15
	)
buffers = GraphiteSqlMetric('metrics.get_buffer_values'
	, path_descriptor='buffers', metric_name='buffers'
	, interval_seconds=15
	)
buffer_nodes = GraphiteSqlMetric('metrics.get_buffer_node_values'
	, path_descriptor='nodes.buffers', metric_name='buffer_nodes'
	, key_columns=['buffer_node']
	, interval_seconds=60
	)
scheduler_waits = GraphiteSqlMetric('metrics.get_scheduler_waits'
	, path_descriptor='nodes.schedulers', metric_name='scheduler_waits'
	, key_columns=['numa_node', 'scheduler_id', 'cpu_id']
	, interval_seconds=15
	)
session_requests = GraphiteSqlMetric('metrics.get_session_requests'
	, path_descriptor='session_requests', metric_name='session_requests'
	, interval_seconds=5
	)

def build_io_result_metric_path(result):
	if result['database_name'] == 'all':
		return "IO.{}.{}.".format(result['database_file_type'], result['physical_drive_letter'])
	else:
		return "IO.by_database.{}.{}.".format(result['database_name'].replace(' ', '_'), result['database_file_type'])

io = GraphiteSqlMetric('metrics.get_read_write_times'
	, metric_name='io'
	, key_columns=['database_name','database_file_type','database_file_name', 'physical_drive_letter']
	, metric_path_function=build_io_result_metric_path
	, interval_seconds=15
	)
