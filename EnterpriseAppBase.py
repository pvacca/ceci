import logging
from logging.handlers import TimedRotatingFileHandler
from os import path

import traceback
import sys

def format_exception(e):
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

    exception_str = "Traceback (most recent call last):\n"
    exception_str += "".join(exception_list)
    # Removing the last \n
    exception_str = exception_str[:-1]

    return exception_str

class EnterpriseLoggingBase(object):
	def __init__(self, **kwargs):
		self._log_settings = kwargs

		if 'logger' not in self._log_settings:
			self._log_settings['logger'] = logging.getLogger(self.logger_name)

		if self.seperate_exception_log:
			self.__init_exception_logger()

		self.logger.setLevel(self.level)

		self._logging_handler = TimedRotatingFileHandler(
			self.logpath, **self.log_rotation)

		self._logging_handler.setLevel(self.level)
		self._logging_handler.setFormatter(logging.Formatter(self.format, self.datefmt))
		self.logger.addHandler(self._logging_handler)

		if self.to_console:
			self._console_handler = logging.StreamHandler()
			self._console_handler.setLevel(self.to_console_level)
			self._console_handler.setFormatter(logging.Formatter(self.format, self.datefmt))
			self.logger.addHandler(self._console_handler)

		#write a message!
		self.debug("Instantiated log - level: %s, at %s", repr(self.level), self.logpath)

	def __init_exception_logger(self):
		"""
		If the intent is to _not_ send stack traces to the general log, then the 
		exception_logger name must have different root than the original logger, 
		or all messages will go to both loggers.
		"""
		if 'exception_logger' not in self._log_settings:
			if self.seperate_exception_log:
				self._log_settings['exception_logger'] = logging.getLogger(
					"exception.%s" % self.logger_name)	

		if self.seperate_exception_log:
			self.exception_logger.setLevel(logging.ERROR)

			self._exception_logging_handler = TimedRotatingFileHandler(
				self.exception_log, **self.log_rotation)

			self._exception_logging_handler.setLevel(logging.ERROR)
			self._exception_logging_handler.setFormatter(
				logging.Formatter(self.format, self.datefmt))
			self.exception_logger.addHandler(self._exception_logging_handler)

	@property
	def logger_name(self):
		return self._log_settings['logger_name'] if 'logger_name' in self._log_settings \
			else 'ClassLoggingBase.%s' % (__name__)

	@property
	def logger(self):
		return self._log_settings['logger']

	@property
	def level(self):
		return self._log_settings['level'] if 'level' in self._log_settings else logging.DEBUG

	@property
	def format(self):
		return self._log_settings['format'] if 'format' in self._log_settings \
			else '%(asctime)s %(levelname)s %(name)s %(thread)s %(message)s'

	@property
	def datefmt(self):
		return self._log_settings['datefmt'] if 'datefmt' in self._log_settings \
			else '%Y%m%d %H:%M:%S'

	@property
	def logging_settings(self):
		return {'logger_name': self.logger_name
			, 'logger': self.logger
			, 'level': self.level
			, 'format': self.format
			, 'datefmt': self.datefmt
			, 'logpath': self.logpath
			, 'rotation': self.log_rotation
			}

	@property
	def log_rotation(self):
		if 'rotation' not in self._log_settings:
			self._log_settings['rotation'] = {'when':'midnight','interval':1
				,'backupCount':0,'encoding':None,'delay':False,'utc':False}

		return self._log_settings['rotation']

	@property
	def seperate_exception_log(self):
		return self._log_settings['seperate_exception_log'] if 'seperate_exception_log' in self._log_settings \
			else False

	@property
	def exception_logger(self):
		return None if not self.seperate_exception_log \
			else self._log_settings['exception_logger']

	@property
	def exception_log(self):
		"""Path to log containing exception messages and stack traces.
		If not specified, defaults to the same path / filename of the logger
		with the word 'exception' inserted before the extension.
		"""
		if not self.seperate_exception_log:
			return None

		if 'exception_log' not in self._log_settings:
			exceptionLogName = self.logname.split('.')
			extension = exceptionLogName[-1]
			exceptionLogName[-1] = 'exception'
			exceptionLogName.append(extension)
			self._log_settings['exception_log'] = path.join(self.logdir
				, '.'.join(exceptionLogName))

		return self._log_settings['exception_log']

	@property
	def to_console(self):
		if 'to_console' not in self._log_settings:
			self._log_settings['to_console'] = False

		return self._log_settings['to_console']

	@property
	def to_console_level(self):
		if 'to_console_level' not in self._log_settings:
			self._log_settings['to_console_level'] = logging.ERROR if self.to_screen \
				else None

		return self._log_settings['to_console_level']

	@property
	def logpath(self):
		"""path to log file; composed from logdir and logname if logpath key not otherwise specified."""
		# return self._logpath
		if 'logpath' not in self._log_settings:
			self._log_settings['logpath'] = path.join(self.logdir, self.logname)

		return self._log_settings['logpath']

	@property
	def logdir(self):
		""" The path to the directory containing log files."""
		if 'logdir' not in self._log_settings:
			self._log_settings['logdir'] = '/tmp' if 'logpath' not in self._log_settings \
				else path.dirname(self._log_settings['logpath'])
		return self._log_settings['logdir']

	@property
	def logname(self):
		""" The filename with extension of the logfile; but with no path information."""
		if 'logname' not in self._log_settings:
			self._log_settings['logname'] = '%s.log' % (self.logger_name) if 'logname' not in self._log_settings \
				else path.basename(self._log_settings['logpath'])
		# print self._log_settings['logpath'], self._log_settings['logname']
		return self._log_settings['logname']

	def debug(self, msg, *args, **kwargs):
		self.logger.debug(msg, *args, **kwargs)

	def info(self, msg, *args, **kwargs):
		self.logger.info(msg, *args, **kwargs)

	def warning(self, msg, *args, **kwargs):
		self.logger.warning(msg, *args, **kwargs)

	def error(self, msg, *args, **kwargs):
		self.logger.error(msg, *args, **kwargs)
		if self.seperate_exception_log:
			self.exception_logger.error(msg, *args, **kwargs)

	def critical(self, msg, *args, **kwargs):
		self.logger.critical(msg, *args, **kwargs)
		if self.seperate_exception_log:
			self.exception_logger.critical(msg, *args, **kwargs)

	def log(self, lvl, msg, *args, **kwargs):
		self.logger.log(lvl, msg, *args, **kwargs)
		if self.seperate_exception_log:
			self.exception_logger.log(lvl, msg, *args, **kwargs)

	def exception(self, msg, *args):
		if self.seperate_exception_log:
			self.logger.error(msg.message)
			self.exception_logger.error(self.exception_logger.findCaller())
			self.exception_logger.error(msg.message)
			self.exception_logger.error(format_exception(msg))
		else:
			self.logger.exception(msg, *args)

from GraphiteBase import GraphiteBase, graphite_kw_set

class EnterpriseAppBase(EnterpriseLoggingBase, GraphiteBase):
	def __init__(self, *args, **kwargs):
		self._graphite_kwargs = dict((kw, kwargs.pop(kw)) \
			for kw in graphite_kw_set if kw in kwargs)

		EnterpriseLoggingBase.__init__(self, **kwargs)
		self.debug("App base initialized.")

	def start_graphite_thread(self, daemon=False):
		self.debug("initializing graphite thread")
		GraphiteBase.__init__(self, **self._graphite_kwargs)
		try:
			self.start_graphite(daemon)
		except (Exception) as e:
			self.exception(e)
			raise
		self.debug("started graphite thread")


	def __del__(self):
		if not self.graphite_stopped():
			self.stop_graphite()

# if __name__ == '__main__':
# 	l = EnterpriseLoggingBase(logger_name='SomeFun'
# 		, logdir='/home/phil/CorvisaCloud'
# 		, toScreen=True)
# 	l.debug("Yeah, I'm a debug message")
# 	l.error("oh nos! dude!")
# 	e = Exception("Dude, WTF?")
# 	l.exception(e)


if __name__ == '__main__':
	from Keyboard import (kbhit, set_normal_term
		, set_curses_term, getch)
	from atexit import register

	register(set_normal_term)
	set_curses_term()

	app = EnterpriseAppBase(server='', port='', interval=5, echo=True
		, logger_name='EnterpriseAppBase'
		, logdir='/home/phil/CorvisaCloud'
		, to_console=True)

	app.start_graphite_thread()
	r = True
	while r:
		try:
			if kbhit():
				ch = getch()
				if ch == ' ' or ch == 'q':
					app.stop_graphite()
					r = False
				else:
					app.error(ch)
					app.add_graphite_value(path='some.graphite.path', value=ch)
		except (KeyboardInterrupt):
			app.stop_graphite()
			r = False
		
	print("kthnxbai")
