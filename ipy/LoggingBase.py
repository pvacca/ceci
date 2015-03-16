from __future__ import print_function

import logging
from logging.handlers import TimedRotatingFileHandler
from os import path, getcwd
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

class LoggingBase(object):


	def __init__(self, **kwargs):
		self._logging_settings = kwargs

		self.__set_logging_defaults()

		if self.rotate:
			handler = TimedRotatingFileHandler(self.logpath, **self.logging_settings['rotation_settings'])
		else:
			handler = logging.FileHandler(self.logpath)

		if self.console:
			shandler = logging.StreamHandler()
			shandler.setLevel(self.logging_settings['console_settings']['logging_level'])
			shandler.setFormatter(logging.Formatter(
				self.logging_settings['console_settings']['format']
				, self.logging_settings['console_settings']['datefmt']
				))
			self.logger.addHandler(shandler)

		self.logger.setLevel(self.logging_level)
		handler.setFormatter(logging.Formatter(self.format, self.datefmt))
		self.logger.addHandler(handler)

	def __set_logging_defaults(self):
		""" Check in the following order: full path to log file (overwrites logname & logpath if also specified)
			, specified logger_name
		"""
		if 'logpath' in self.logging_settings:
			# partition on last seperator
			self._logging_settings['logdir'] = path.dirname(path.realpath(self.logpath))
			self._logging_settings['logname'] = path.basename(path.realpath(self.logpath))

		if 'logger_name' not in self.logging_settings:
			if 'logname' not in self.logging_settings:
				self._logging_settings['logger_name'] = 'LoggingBase.%s' % (__name__)
			else:
				self._logging_settings['logger_name'] = self.logname[:-4]

		if 'logdir' not in self.logging_settings:
			self._logging_settings['logdir'] = getcwd()

		if 'logname' not in self.logging_settings:
			self._logging_settings['logname'] = "{}.log".format(self.logger_name)

		if 'logpath' not in self.logging_settings:
			self._logging_settings['logpath'] = path.join(self.logdir, self.logname)

		if 'logger' not in self.logging_settings:
			self._logging_settings['logger'] = logging.getLogger(self.logger_name)

		if 'logging_level' not in self.logging_settings:
			self._logging_settings['logging_level'] = logging.DEBUG

		if 'format' not in self.logging_settings:
			self._logging_settings['format'] = '%(asctime)s %(levelname)s %(name)s %(thread)s %(message)s'

		if 'datefmt' not in self.logging_settings:
			self._logging_settings['datefmt'] = '%Y%m%d %H:%M:%S'

		if 'rotate' not in self.logging_settings:
			self._logging_settings['rotate'] = True

		if self.rotate and 'rotation_settings' not in self.logging_settings:
			self._logging_settings['rotation_settings'] = {
				'when': 'midnight'
				, 'interval': 1
				, 'backupCount': 0
				, 'encoding': None
				, 'delay': False
				, 'utc': False
				}

		if 'console' not in self.logging_settings:
			self._logging_settings['console'] = False

		if self.console:
			if 'console_settings' not in self.logging_settings:
				self._logging_settings['console_settings'] = {}
			for a in ['logging_level','format','datefmt']:
				if a not in self.logging_settings['console_settings']:
					self.logging_settings['console_settings'][a] = self.logging_settings[a]

	@staticmethod
	def log_to(level, log_with_params=False):
		"""
		Intended be used as a decorator function which logs the class & function name at a specified logging level, 
		or logs to debug if the specified level is not a valid logging level.  Optionally logs invoked parameters.
		"""
		def wrapper(func):
			def wrapped(self, *args, **kwargs):
				try:
					logging_method = getattr(self, level)
				except AttributeError:
					self.error("Attempt to log to level {} raised AttributeError".format(level))
					logging_method = self.debug

				message = "{} called {}".format(self.__class__.__name__, func.__name__)
				if log_with_params:
					message += " ({})".format([args, kwargs])
				logging_method(message)

				return func(self, *args, **kwargs)
			wrapped.__name__ = func.__name__
			wrapped.__doc__ = func.__doc__
			return wrapped
		return wrapper

	logging_settings = property(lambda self: self._logging_settings)

	logger = property(lambda self: self._logging_settings['logger'])

	logger_name = property(lambda self: self._logging_settings['logger_name'])

	logging_level = property(lambda self: self._logging_settings['logging_level'])

	format = property(lambda self: self._logging_settings['format'])

	datefmt = property(lambda self: self._logging_settings['datefmt'])

	rotate = property(lambda self: self._logging_settings['rotate'])

	logdir = property(lambda self: self._logging_settings['logdir'])

	logname = property(lambda self: self._logging_settings['logname'])

	logpath = property(lambda self: self._logging_settings['logpath'])

	console = property(lambda self: self._logging_settings['console'])

	def debug(self, msg, *args, **kwargs):
		self.logger.debug(msg, *args, **kwargs)

	def info(self, msg, *args, **kwargs):
		self.logger.info(msg, *args, **kwargs)

	def warning(self, msg, *args, **kwargs):
		self.logger.warning(msg, *args, **kwargs)

	def error(self, msg, *args, **kwargs):
		self.logger.error(msg, *args, **kwargs)

	def critical(self, msg, *args, **kwargs):
		self.logger.critical(msg, *args, **kwargs)

	def log(self, lvl, msg, *args, **kwargs):
		self.logger.log(lvl, msg, *args, **kwargs)

	def exception(self, msg, *args):
		self.logger.exception(msg, *args)
