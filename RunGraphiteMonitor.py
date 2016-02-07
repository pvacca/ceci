from Metrics import *
from SqlGraphite import SqlMonitorGraphiteRunner
from SqlMonitor import SqlServerMonitor
from DefaultGraphiteSettings import logging_settings

from System import Console, ConsoleKey

srvr_services1 = SqlServerMonitor('services1.mydomain', graphite_root='msdb', **logging_settings)

srvr_services1.db = 'dba2'
srvr_services1.add_metric(blocks_waits).add_metric(wait_stats).add_metric(async_network_waits).add_metric(
	sql_server_statistics).add_metric(database_statistics).add_metric(connections).add_metric(
	clr_execution).add_metric(lock_stats).add_metric(latch_wait_stats).add_metric(memory).add_metric(
	buffers).add_metric(buffer_nodes).add_metric(scheduler_waits).add_metric(session_requests).add_metric(io)

graphite = SqlMonitorGraphiteRunner(logpath=logging_settings['logpath'])

graphite.add_server(srvr_services1)

graphite.echo = False
graphite.start('graphite1.mydomain', 2003)

while True:
	c = Console.ReadKey(True)
	if c.Key in [ConsoleKey.Escape, ConsoleKey.Q]:
		break

graphite.quit()

print('\nKthnxbai')
