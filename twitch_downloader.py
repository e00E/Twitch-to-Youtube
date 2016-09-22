import logging
import os
import requests
import time
from random import random
import m3u8


twitch_api_url = "https://api.twitch.tv"
twitch_usher_url = "http://usher.twitch.tv"

def get_session(video_id, headers=dict()):
	r = requests.get(twitch_api_url + "/api/vods/{}/access_token".format(video_id), headers=headers)
	logging.debug("get_session for video_id {} got data {}".format(video_id, r.content))
	json = r.json()
	return (json['token'], json['sig'])

def get_variant_playlist(video_id, headers=dict()):
	token, sig = get_session(video_id, headers)
	params = {
		"player": "twitchweb",
		"p": int(random() * 999999),
		"type": "any",
		"allow_source": "true",
		"allow_audio_only": "true",
		"nauth": token,
		"nauthsig": sig }

	r = requests.get(twitch_usher_url + "/vod/{}".format(video_id), params=params)
	r.encoding = 'utf-8'
	logging.debug('get_variant_playlist for video_id {} got data {}'.format(video_id, r.content))
	#Some playlists have bandwidth set to none which is not valid m3u8 and will crash the parser
	#we dont care about the value so fix it by setting it something
	text = r.text.replace(',BANDWIDTH=None,', ',BANDWIDTH=1,')
	return m3u8.loads(text)

def get_source_playlist(video_id, headers=dict()):
	variant_playlist = get_variant_playlist(video_id, headers)
	for playlist in variant_playlist.playlists:
		for media in playlist.media:
			if media.group_id == 'chunked': # Corresponds to Source quality
				uri = playlist.uri
				base_path = playlist.uri[:playlist.uri.rfind('/')]
				r = requests.get(uri)
				r.encoding = 'utf-8'
				logging.info('get_source_playlist found source playlist for video_id {} at {}'.format(video_id, playlist.uri))
				logging.debug('get_source_playlist source playlist data is {}'.format(r.content))
				playlist = m3u8.M3U8(content=r.text, base_path=base_path)
				return playlist

def download_video(video_id, filename, chunk_size = 2**20, timeout=12.1, callback_progress_update=None):
	'''	video_id is just a string of numbers and does not start with a v'''
	logging.info('Downloading video {} as  {}'.format(video_id, filename))
	with open(filename, 'wb') as file, requests.Session() as session:
		segments = get_source_playlist(video_id).segments.uri
		for segment in segments:
			logging.debug('download_video is now now downloading segment {}'.format(segment))
			while True: # Keep trying to download the segment until success
				written_bytes_segment = 0
				try:
					r = session.get(segment, stream=True, timeout=timeout)
					for content in r.iter_content(chunk_size=chunk_size):
						logging.debug('download_video finished downloading a chunk')
						file.write(content)
						written_bytes_segment += len(content)
						if callback_progress_update is not None:
							callback_progress_update(file.tell())
					break
				except requests.exceptions.RequestException as e:
					logging.warning('download_video encountered the following exception while trying to download segment {} {}'.format(segment, e))
					logging.info('will retry download after {} seconds'.format(timeout))
					if written_bytes_segment > 0: # Remove what was written from this segment so we do not write it multiple times when retrying
						file.seek(file.tell() - written_bytes_segment)
						file.truncate()
					time.sleep(timeout)
					continue
