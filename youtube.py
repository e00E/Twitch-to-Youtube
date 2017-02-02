import random
import time
import httplib2
import logging

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload, MediaIoBaseUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

httplib2.RETRIES = 1


VALID_PRIVACY_STATUSES = ("public", "private", "unlisted")

class YoutubeUploader:
	def __init__(self, auth_file, client_secrets_file=None):
		YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube"
		YOUTUBE_API_SERVICE_NAME = "youtube"
		YOUTUBE_API_VERSION = "v3"

		args = argparser.parse_args("")
		args.noauth_local_webserver = True


		storage = Storage(auth_file)
		credentials = storage.get()

		if credentials is None or credentials.invalid:
			assert client_secrets_file, "Auth file not valid but not client secrets file specified"
			flow = flow_from_clientsecrets(
				client_secrets_file,
				scope=YOUTUBE_UPLOAD_SCOPE,
				redirect_uri='urn:ietf:wg:oauth:2.0:oob')
			credentials = run_flow(flow, storage, args)

		self.youtube_api = build(
			YOUTUBE_API_SERVICE_NAME,
			YOUTUBE_API_VERSION,
			http=credentials.authorize(httplib2.Http()))
	def create_playlist(self, title, description='', privacyStatus="private"):
		assert(privacyStatus in ["public", "private"])
		response = self.youtube_api.playlists().insert(
			part="snippet,status",
			body=dict(
				snippet=dict(
					title=title,
					description=description),
				status=dict(
					privacyStatus=privacyStatus))).execute()
		return response
	def add_to_playlist(self, playlist_id, video_id):
		response = self.youtube_api.playlistItems().insert(
			part="snippet",
			body=dict(
				snippet=dict(
					playlistId=playlist_id,
					resourceId=dict(
						kind="youtube#video",
						videoId=video_id)))).execute()
		return response
	def file_to_media_body(filename):
		return MediaFileUpload(filename, chunksize=-1, resumable=True)
	def iobase_to_media_body(iobase):
		return MediaIoBaseUpload(iobase, mimetype='application/octet-stream', chunksize=-1, resumable=True)
	def upload(self, media_body, title=None, description=None, category=None, tags=None, privacyStatus="private"):
		assert(privacyStatus in ["public", "private", "unlisted"])
		def shorten_str_to_bytes(max_bytes, string):
			while len(string.encode()) > max_bytes:
				string = string[:-1]
			return string

		snippet = dict()
		if title:
			title = title.replace('<', '}')
			title = title.replace('>', '{')
			encoded_title = title.encode()
			if len(encoded_title) > 100:
				logging.warning("Title is longer than Youtube limit of 100 bytes, shortening it")
				# Keep removing last character and re-encoding until encoded size is short enough
				title = shorten_str_to_bytes(100, title)
			snippet["title"] = title
		if description:
			# these brackets are not allowed in youtube descriptions
			description = description.replace('<', '}')
			description = description.replace('>', '{')
			if len(description.encode()) > 5000:
				logging.warning("Description is longer than Youtube limit of 5000 bytes, shortening it")
				description = shorten_str_to_bytes(5000, description)
			snippet["description"] = description
		if category: snippet["categoryId"] = category
		if tags: snippet["tags"] = tags
		body=dict(
			snippet = snippet,
			status = dict(
				privacyStatus = privacyStatus))

		def upload_process():
			insert_request = self.youtube_api.videos().insert(
				part=",".join(body.keys()),
				body=body,
				media_body=media_body)

			response = None
			error = None
			retry = 0
			while response is None:
				try:
					status, response = insert_request.next_chunk()
					if 'id' in response:
						print("Video id '%s' was successfully uploaded." % response['id'])
						return response["id"]
					else:
						raise RuntimeException("The upload failed with an unexpected response: %s" % response)
				except HttpError as e:
					if e.resp.status in [500, 502, 503, 504]:
						error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
					else:
						raise
				except httplib2.HttpLib2Error as e:
					error = "A retriable error occurred: %s" % e

				if error is not None:
					print(error)
					retry += 1
					if retry > 10:
						exit("No longer attempting to retry.")

				max_sleep = 2 ** retry
				sleep_seconds = random.random() * max_sleep
				print("Sleeping %f seconds and then retrying..." % sleep_seconds)
				time.sleep(sleep_seconds)

		result = None
		while result is None: #Retry the whole upload until True is returned
			try:
				print('debug starting upload')
				result = upload_process()
			except (HttpError, ConnectionError) as e:
				print("Unretriable error occured while uploading:")
				print(e)
				print("retrying in 60 seconsd")
				time.sleep(60)
		print("Finished upload with id %s" % result)
		return result
