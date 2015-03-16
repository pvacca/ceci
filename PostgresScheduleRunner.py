from __future__ import (division, print_function, unicode_literals)
from datetime import datetime, timedelta

from PostgresBase import PostgresConnectionBase
from TimerThread import RecurringTimerThread

class ScheduledItem(Object):
	def __init__(self):
		pass

	@property
	def interval(self):
		return self._interval

	@interval.setter
	def interval(self, **kwargs):
		self._interval = timedelta(**kwargs)

	@property
	def last_run(self):
		return self._last_run

	@property
	def next_run(self):
		return self.last_run + self.interval

class PostgresScheduleRunner(PostgresConnectionBase):

	def __init__(self):
		self._schedule_poller = RecurringTimerThread(1, self.check)

	def add_item(self, item):
		pass

	def check(self):
		pass