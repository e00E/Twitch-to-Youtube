# Implement IOBase for a Twitch video

from io import IOBase
import bisect
import twitch_downloader
import requests
import time
import logging
import sys


# TODO can make a bit faster by also caching the next chunk early in a seperate thread

# This class provides an IOBase interface to a Twitch video
# This means the contents of the video can be accessed like a file with reading and seeking.
# To achieve it we parse the urls in the playlist
# and keep lists of which chunk contains what byte offset (and video duration).
# With that list we know the total size of the video and can map an offset to a chunk via bisecting the list.
# The class always caches the last used chunk.

# The class was created to allow videos to be downloaded from twitch and uploaded to youtube
# without needing to keep the whole files on disk.
class TwitchIO(IOBase):
	def __init__(self, video_id):
		# video_id is just a string of numbers and does not start with a v
		self.segments = twitch_downloader.get_source_playlist(video_id).segments
		self.session = requests.Session()
		self.build_index()
		self.position = 0
		self.index = None
		# Cache of last downloaded chunk
		self.last_chunk_index = None
		self.last_chunk = None
	def build_index(self):
		self.offset_index = list()
		self.size = 0
		self.time_index = list()
		self.duration = 0.0
		for segment in self.segments:
			# Some older vods dont have start and end offsets in the playlist
			# so for those we need to send head requests to get the chunk size
			uri = segment.uri
			parameters = uri[uri.rfind('?')+1:]
			parameters = parameters.split('&')
			keyvalues = dict()
			for i in parameters:
				try:
					(key, value) = i.split('=')
					keyvalues[key] = int(value)
				except ValueError as e:
					continue
			if 'start_offset' in keyvalues and 'end_offset' in keyvalues:
				self.size += keyvalues['end_offset'] - keyvalues['start_offset'] + 1 # + 1 because those ranges are inclusive
			else:
				while True:
					try:
						response = self.session.head(segment.uri, timeout=12.1)
						response.raise_for_status()
						size = int(response.headers['Content-Length'])
						self.size += size
						break
					except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
						logging.warning('Encounted following exception while trying to HEAD chunk {} {}'.format(index, e))
						time.sleep(12.1)
						continue
			duration = segment.duration
			self.offset_index.append(self.size)
			self.duration += duration
			self.time_index.append(self.duration)
	def seek(self, offset, whence=0):
		if whence == 0:
			pos = offset
		elif whence == 1:
			pos = self.position + offset
		elif whence == 2:
			pos = self.size + offset
		else:
			raise RuntimeError('Unrecognized whence constant')
		if pos > self.size: pos = self.size
		self.index = self.get_index_for_offset(pos) if pos < self.size else None
		self.position = pos
	def get_index_for_offset(self, offset):
		assert(offset >= 0)
		assert(offset < self.size)
		return bisect.bisect(self.offset_index, offset)
	def read_chunk(self, index):
		if index == self.last_chunk_index:
			return self.last_chunk
		while True:
			try:
				response = self.session.get(self.segments[index].uri, timeout=12.1)
				response.raise_for_status()
				self.last_chunk_index = index
				self.last_chunk = response.content
				return self.last_chunk
			except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
				logging.warning('Encounted following exception while trying to download chunk {} {}'.format(index, e))
				time.sleep(12.1)
				continue
	def read(self, size=-1):
		print('\rread', self.position, self.size, size, '              ', end='')
		assert(size == -1 or size >= 0)
		end_position = self.size if size == -1 else min(self.position + size, self.size)

		result = bytearray()

		while self.position < end_position:
			self.index = self.get_index_for_offset(self.position)

			chunk_start = self.offset_index[self.index - 1] if self.index > 0 else 0
			chunk_end = self.offset_index[self.index]
			chunk_size = chunk_end - chunk_start
			bytes_left_in_chunk = chunk_end - self.position
			assert(bytes_left_in_chunk > 0)
			chunk = self.read_chunk(self.index)
			chunk_pos = self.position - chunk_start

			number_of_bytes_to_read = min(end_position - self.position, chunk_size - chunk_pos)
			result += chunk[chunk_pos:chunk_pos + number_of_bytes_to_read]
			self.position += number_of_bytes_to_read

		return result
	def seekable(self):
		return True
	def readable(self):
		return True
	def tell(self):
		return self.position
	def writeable(self):
		return False
