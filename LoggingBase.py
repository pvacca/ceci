from __future__ import print_function
import logging
from os import path

# todo: add future print_function
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
    
class ClassLoggingBase(object):
	def __init__(self, **kwargs):
			# , logger
			# , level=logging.DEBUG
			# , format='%(asctime)s %(levelname)s %(filename)s %(name)s %(thread)s %(message)s'
			# , datefmt='%y%m%d %H:%M:%S'
			# , logdir=None
			# , logname=None
			# , logpath=None):
		self._settings = kwargs

		if 'logger' not in self._settings:
			self._settings['logger']=logging.getLogger(self.logger_name)

		self.logger.setLevel(self.level)

		# todo: change to rotating filehandler
		self._logging_handler = logging.FileHandler(self.logpath, mode='a')
		self._logging_handler.setLevel(self.level)
		self._logging_handler.setFormatter(logging.Formatter(self.format, self.datefmt))
		self.logger.addHandler(self._logging_handler)
		self.debug("%s, %s", repr(self.level), self.logpath) #do write a message!

	@property
	def logger_name(self):
		return self._settings['logger_name'] if 'logger_name' in self._settings \
			else 'ClassLoggingBase.%s' % (__name__)

	@property
	def logger(self):
		return self._settings['logger']

	@property
	def level(self):
		return self._settings['level'] if 'level' in self._settings else logging.DEBUG

	@property
	def format(self):
		return self._settings['format'] if 'format' in self._settings \
			else '%(asctime)s %(levelname)s %(name)s %(thread)s %(message)s'

	@property
	def datefmt(self):
		return self._settings['datefmt'] if 'datefmt' in self._settings \
			else '%y%m%d %H:%M:%S'

	@property
	def logpath(self):
		"""path to log file; composed from logdir and logname if logpath key not otherwise specified."""
		# return self._logpath
		return self._settings['logpath'] if 'logpath' in self._settings \
			else path.join(self._settings['logdir'] if 'logdir' in self._settings else '/tmp'
				, self._settings['logname'] if 'logname' in self._settings \
					else '%s.log' % (self.logger_name)
				)

	@property
	def logging_settings(self):
		return {'logger_name':self.logger_name
			, 'logger':self.logger
			, 'level':self.level
			, 'format':self.format
			, 'datefmt':self.datefmt
			, 'logpath':self.logpath
			}

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

	# def debug(self, msg, *args):
	# 	self.logger.debug("%s" % (msg % args))

	# def info(self, msg, *args):
	# 	self.logger.info("%s" % (msg % args))

	# def warning(self, msg, *args):
	# 	self.logger.warning("%s" % (msg % args))

	# def error(self, msg, *args):
	# 	self.logger.error("%s" % (msg % args))

	# def critical(self, msg, *args):
	# 	self.logger.critical("%s" % (msg % args))
