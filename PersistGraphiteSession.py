from Metrics import *
from SqlGraphite import SqlMonitorGraphiteRunner
from SqlMonitor import SqlServerMonitor
from GraphiteFormulaBuilder import GraphiteSqlPersist, GraphiteMetricName, GraphiteFormulaBuilder as Formula
from DefaultGraphiteSettings import logging_settings

from System import Console, ConsoleKey

srvr_services1 = SqlServerMonitor('services1.chcgil1.it.corp', graphite_root='msdb', **logging_settings)

srvr_services1.db = 'dba'
# srvr_services1.add_metric(blocks_waits).add_metric(wait_stats).add_metric(async_network_waits).add_metric(
# 	sql_server_statistics).add_metric(database_statistics).add_metric(connections).add_metric(
# 	clr_execution).add_metric(lock_stats).add_metric(latch_wait_stats).add_metric(memory).add_metric(
# 	buffers).add_metric(buffer_nodes).add_metric(scheduler_waits).add_metric(session_requests).add_metric(io)
srvr_services1.add_metric(
		database_statistics
	).add_metric(
		wait_stats
	).add_metric(
		io
	)

graphite = SqlMonitorGraphiteRunner(logpath=logging_settings['logpath'])

graphite.add_server(srvr_services1)

graphite_to_sql = GraphiteSqlPersist(
	'Test persistance & formula builder'
	, 'services1.chcgil1.it.corp'
	, 'dba2'
	, duration_seconds=150
	, export_interval_seconds=30
	# , logger_name='GraphitePersistance'
	, logpath=r'e:\templogs\GraphitePersistance.log'
	, event_source_name='GraphiteMonitor'
	# , suppress_event_logging=True
	)

CorvisaReportingLogFlushes = GraphiteMetricName(
	'services1.chcgil1.it.corp'
	, 'database_statistics'
	, 'log_flush_perSec'
	, database_name='CorvisaReporting'
	)

Services1SosWaitTime = GraphiteMetricName(
	'services1.chcgil1.it.corp'
	, 'wait_stats'
	, 'sos_scheduler_yield_wait_time_ms'
	)

Services1_SDriveRW_Stall = GraphiteMetricName(
	'services1.chcgil1.it.corp'
	, 'io'
	, 'io_stall'
	, database_name='all', database_file_type='by_drive', physical_drive_letter='S'
	)

Services1_SDriveRW = GraphiteMetricName(
	'services1.chcgil1.it.corp'
	, 'io'
	, 'reads_writes'
	, database_name='all', database_file_type='by_drive', physical_drive_letter='S'
	)

# AvgLogFlush_perSec = Formula('CorvisaReporting Avg Log Flushes per second')
# AvgLogFlush_perSec.add_series(CorvisaReportingLogFlushes, apply_formula='granular_diff_formula')
AvgLogFlush_perSec = (Formula('CorvisaReporting Avg Log Flushes per second')).add_series(CorvisaReportingLogFlushes, apply_formula='granular_diff_formula')

SosWait = Formula('services1 sos scheduler wait time')
SosWait.add_series(Services1SosWaitTime, apply_formula='granular_diff_formula')

DataRWLatency = Formula('Data file read/write latency (ms)')
DataRWLatency.add_series(
		Services1_SDriveRW_Stall, apply_formula='granular_diff_formula'
	).divide_series(
		Services1_SDriveRW, apply_formula='granular_diff_formula'
	)

graphite_to_sql.add_formula(
		AvgLogFlush_perSec
	).add_formula(
		SosWait
	).add_formula(
		DataRWLatency
	)

# graphite.echo = False
graphite.silent = True
# graphite.start('graphite1.chcgil1.it.corp', 2003)
graphite.start('graphite.it.corp', 2003)
graphite_to_sql.start(graphite)

while graphite_to_sql.is_running:
	c = Console.ReadKey(True)
	if c.Key in [ConsoleKey.Escape, ConsoleKey.Q]:
		graphite_to_sql.quit()
		break

print('\n\n\tOUT')

graphite.quit()

print('\nKthnxbai')
