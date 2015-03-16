class HandlingSkeleton(object):
	def __init__(self):
		self._handled_count = 0
		self._failed_count = 0
		self._total_message_count = 0

	@property
	def handled(self):
		return self._handled_count

	@handled.setter
	def handled(self, i):
		self._handled_count = i

	@property
	def failed(self):
		return self._failed_count

	@failed.setter
	def failed(self, i):
		self._failed_count = i

	@property
	def messages(self):
		return self._total_message_count

	@messages.setter
	def messages(self, i):
		self._total_message_count = i
