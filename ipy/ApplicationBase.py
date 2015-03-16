import logging

from System.Diagnostics import EventLog, EventLogEntryType

from LoggingBase import LoggingBase, format_exception

# if not EventLog.SourceExists(event_source_name):
# 	EventLog.CreateEventSource(event_source_name)
# ^^ requires admin privelege. ^^  it's pretty much easier to open an admin powershell console and run:
# 
# $source = "GraphiteMonitor"
# If(![System.Diagnostics.EventLog]::SourceExists($source))
# {
# 	[System.Diagnostics.EventLog]::CreateEventSource($source, "Application")
# }

class WindowsEventLogHelper(object):
	def __init__(self, event_source_name, computer_name='.', event_log_name='Application'):
		""" Cannot create a new named log for no real reason except that windows is stupid.
			Available logs are Application, Setup, & System (so default to Application).
		"""
		self._event_source_name = event_source_name
		self._computer_name = computer_name
		self._event_log_name = event_log_name

	event_source_name = property(lambda self: self._event_source_name)

	event_log_name = property(lambda self: self._event_log_name)

	def WriteEvent(self, message):
		""" Write an informational message to the initialized event log."""
		with EventLog(self._event_log_name, self._computer_name, self._event_source_name) as ev:
			ev.WriteEntry(message, EventLogEntryType.Information)

	def WriteWarning(self, message):
		""" Write a warning message to the initialized event log."""
		with EventLog(self._event_log_name, self._computer_name, self._event_source_name) as ev:
			ev.WriteEntry(message, EventLogEntryType.Warning)

	def WriteError(self, message):
		""" Write an error message to the initialized event log."""
		with EventLog(self._event_log_name, self._computer_name, self._event_source_name) as ev:
			ev.WriteEntry(message, EventLogEntryType.Error)

class WindowsAppLoggingBase(WindowsEventLogHelper, LoggingBase):


	def __init__(self, *args, **kwargs):
		""" Initializes a combined class that both writes to a python event log or logs (generally a text log), as well as
			the windows event log. The kwarg "event_logging_level" can be set if the python log should log lower level events
			than should the Windows Event Log; i.e. INFO & above to the python log, ERROR and above to the Windows Event Log.
			The reverse is not possible, since that is clearly a ridiculous thing to do ;)

			Since we must initialize LoggingBase before WindowsEventLogHelper to use the potentially calculated values of 
			logger_name & (logging)level, there is no call to super().  As a result, the kwargs that may pertain to 
			WindowsEventLogHelper or WindowsAppLoggingBase must be popped from kwargs dict so as not to confuse things.
		"""
		event_log_helper_kwargs = ('event_source_name', 'computer_name', 'event_log_name')

		elh_kwargs = dict([(kw, kwargs.pop(kw)) for kw in event_log_helper_kwargs if kw in kwargs])

		LoggingBase.__init__(self, *args, **kwargs)

		if 'suppress_event_logging' not in kwargs:
			if 'event_source_name' not in elh_kwargs:
				elh_kwargs['event_source_name'] = self.logger_name

			self._event_logging_level = kwargs.pop('event_logging_level') if 'event_logging_level' in kwargs \
				else self.logging_level if self.logging_level > logging.DEBUG \
					else logging.INFO

			WindowsEventLogHelper.__init__(self, **elh_kwargs)
		else:
			# suppress Windows Event Log logging
			self.info = getattr(LoggingBase, 'info')
			self.warning = getattr(LoggingBase, 'warning')
			self.error = getattr(LoggingBase, 'error')
			self.critical = getattr(LoggingBase, 'critical')
			self.exception = getattr(LoggingBase, 'exception')

	event_logging_level = property(lambda self: self._event_logging_level)

	def info(self, msg, *args, **kwargs):
		if self.event_logging_level <= logging.INFO:
			self.WriteEvent(msg)
		self.logger.info(msg, *args, **kwargs)

	def warning(self, msg, *args, **kwargs):
		if self.event_logging_level <= logging.WARNING:
			self.WriteEvent(msg)
		self.logger.warning(msg, *args, **kwargs)

	def error(self, msg, *args, **kwargs):
		if self.event_logging_level <= logging.ERROR:
			if isinstance(msg, str):
				self.WriteError(msg)
			else:
				self.WriteError(format_exception(msg))
		self.logger.error(msg, *args, **kwargs)

	def critical(self, msg, *args, **kwargs):
		if self.event_logging_level <= logging.CRITICAL:
			if isinstance(msg, str):
				self.WriteError(msg)
			else:
				self.WriteError(format_exception(msg))
		self.logger.critical(msg, *args, **kwargs)

	def exception(self, msg, *args):
		if isinstance(msg, str):
			self.WriteError(msg)
		else:
			self.WriteError(format_exception(msg))
		self.logger.exception(msg, *args)
