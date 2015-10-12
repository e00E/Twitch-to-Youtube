import random
import time
import httplib2

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

httplib2.RETRIES = 1


VALID_PRIVACY_STATUSES = ("public", "private", "unlisted")

class YoutubeUploader:
	def __init__(self, auth_file, client_secrets_file=None):
		YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
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
	def upload(self, filename, title=None, description=None, category=None, tags=None, privacyStatus="private"):
		assert(privacyStatus in ["public", "private", "unlisted"])

		snippet = dict()
		if title: snippet["title"] = title
		if description: snippet["description"] = description
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
				media_body=MediaFileUpload(filename, chunksize=-1, resumable=True))

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
					elif e.resp.status == 404:
						return None
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

		result = upload_process()
		while(result == None): #Retry the whole upload until True is returned
			print("Retrying whole upoad because 404 error")
			time.sleep(10)
			result = upload_process()
		print("Finished upload with id %s" % result)
		return result
