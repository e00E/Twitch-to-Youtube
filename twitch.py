import argparse
import subprocess
import requests
import os
import io

from youtube import YoutubeUploader
import twitch_downloader
from TwitchIO import TwitchIO

headers_v3 = {
    'Accept': 'application/vnd.twitchtv.v3+json',
    'Client-ID': '' }

default_tags = ["twitch","stream","vod","vods","broadcast","broadcasts","archive","archives","recording","recordings"]

def json_to_video( json ):
# Convert the json description of a video into a dict.
	return {
		'url': json['url'],
		'id': json['_id'],
		'channel_id': json['channel']['name'],
		'channel_name': json['channel']['display_name'],
		'title': json['title'],
		'description': json['description'],
		'recorded_at': json['recorded_at'],
		'length': json['length'],
		'game': json['game'],
		'status': json['status'] }

def get_video( id ):
# Return a video in the internal format. The video is specified by its video id according to the Twitch api.
	url = 'https://api.twitch.tv/kraken/videos/{}'.format( id )
	r = requests.get( url, headers=headers_v3 )
	json = r.json()
	return json_to_video( json )


def get_videos( channel_name, last_video=None ):
	#last video is none or the id of a video
	#if it is not none only videos that come before last video are returned
	url = 'https://api.twitch.tv/kraken/channels/{}/videos'.format( channel_name )
	videos_per_page = 100 #100 is the maximum videos we can request per call
	payload = { 'limit': videos_per_page,
                    'offset': 0,
                    'broadcasts': 'true',
                    'hls': True }
	def get_page( pagenumber ):
		_payload = payload.copy()
		_payload['offset'] = pagenumber * videos_per_page
		r = requests.get( url, params=_payload, headers=headers_v3)
		json = r.json()
		videos = json['videos']
		result = []
		for v in videos:
			video = json_to_video( v )
			if video['status'] != 'recording':
				result.append(video) #Skip a video if it is currently live
			else:
				print('Skipped video {} because it is still recording aka live'.format(video['id']))
		return result
	videos = []
	index = 0
	page = get_page( index )
	while len(page) > 0:
		for v in page:
			if last_video != None and v['id'] == last_video:
				return videos
			videos.append( v )
		index += 1
		page = get_page( index )
	return videos

def get_video_title(video, part_number=None):
	title = '{} stream from {}'.format( video['channel_name'], video['recorded_at'] )
	if part_number is not None:
		title += ' part {}'.format( part_number )
	return title

def upload_video( video, args, youtube_uploader):
	if args.dont_use_default_tags:
		tags = args.tags.split(",")
	else:
		tags = default_tags + args.tags.split(",")
	title = get_video_title(video)
	description = 'Original title: {}\nOriginal description: {}\nOriginal date: {}\nOriginal Twitch id: {}'.format(
		video['title'],
		video['description'],
		video['recorded_at'], video['id'] )

	print('Creating TwitchIO for', video['id'])
	twitchio = TwitchIO.from_twitch(video['id'][1:])
	print('Video has size {}, real duration {}, twitch duration {}.'.format(twitchio.size, twitchio.duration, video['length']))
	if twitchio.size == 0 or twitchio.duration == 0.0:
		print('Skipping video because size or duration is 0.')
		return
	if twitchio.size > args.max_size or twitchio.duration > args.max_duration:
		if twitchio.size > args.max_size: print('Video is over size limit of {}.'.format(args.max_size))
		if twitchio.duration > args.max_duration: print('Video is over duration limit of {}.'.format(args.max_duration))
		parts = [i for i in twitchio.split_parts(args.max_size, args.max_duration)]
		print('Therefore splitting in {} parts.'.format(len(parts)))
		if not args.dont_use_playlist:
			playlist_id = youtube_uploader.create_playlist(get_video_title(video), privacyStatus=args.privacy)['id']
			print('Created playlist with id {} for parts.'.format(playlist_id))
		for i, part in enumerate(parts):
			part_title = title + ' part {}'.format(i+1)
			media_body = YoutubeUploader.iobase_to_media_body(part)
			print('Starting upload of part {}.'.format(i))
			youtube_video_id = youtube_uploader.upload(media_body, part_title, description, "20", tags, args.privacy)
			print('Finished uploading part as {}.'.format(youtube_video_id))
			if not args.dont_use_playlist:
				youtube_uploader.add_to_playlist(playlist_id, youtube_video_id)
	else:
		media_body = YoutubeUploader.iobase_to_media_body(twitchio)
		print("Starting upload")
		youtube_video_id = youtube_uploader.upload(media_body, title, description, "20", tags, args.privacy)
		print( "Done uploading", video['id'], "as", youtube_video_id )

def process_single_video( video, youtube_uploader, args ):
	# TODO splitting
	upload_video( video, args, youtube_uploader )
	if args.state_file:
		with open( args.state_file, 'w' ) as state_file:
			state_file.writelines( [video['id'] + '\n'] )

if __name__ == "__main__":
	parser = argparse.ArgumentParser( description='Automatically upload twitch vods to youtube.' )
	parser.add_argument( '--authentication-file', help='The file used to authenticate with youtube.', required=True )
	parser.add_argument( '--client-secrets-file', help='Youtube developer api client secrets file. Only needed if auth file is not valid anymore.', required=False )
	parser.add_argument( '--state-file', help='The file that contains the state for channel uploads.' )
	parser.add_argument( '--upload-type', choices=['channel', 'video'], help='Upload a whole channel or a single video.', required=True )
	parser.add_argument( '--destination-id', help='Channel or video id of location to be processed.', required=True )
	parser.add_argument( '--tags', help='Addtional tags for the uploaded videos, comma separated.', default='')
	parser.add_argument( '--dont-use-default-tags', help='Do not add the default tags to the video.', action='store_true' )
	parser.add_argument( '--start-after', help='When in channel mode process only recordings newer than this video id' )
	parser.add_argument( '--max-duration', help='Split videos in parts of duration in seconds.', type=float, default=60*60*11 ) # default is max duration for youtube videos
	parser.add_argument( '--max-size', help='Split videos in parts of size in bytes.', type=int, default=64*(2**30) ) # default is max size for uploading videos to youtube via the api
	parser.add_argument( '--client-id', help='Your twitch application\'s client id', required=True )
	parser.add_argument( '--dont-use-playlist', help='Do not automatically create a playlist for videos that get split in multiple parts.', action='store_true')
	parser.add_argument( '--privacy', help='Upload videos as public, unlisted or private', choices=['public', 'unlisted', 'private'], default='private')
	parser.add_argument( '--game-filter', help='When in channel mode, upload only videos where game matches.', required=False)
	args = parser.parse_args()

	headers_v3['Client-ID'] = args.client_id

	youtube_uploader = YoutubeUploader(args.authentication_file, args.client_secrets_file)

	if args.upload_type == 'channel':
		if args.state_file and args.start_after:
			raise RuntimeError("Provided both a state file and the start_after argument")
		start_after = None
		if args.state_file:
			with open( args.state_file, 'r' ) as state_file:
				start_after = state_file.readline().rstrip('\n')
		else:
			start_after = args.start_after
		videos = get_videos( args.destination_id, start_after )
		videos.reverse()
		for video in videos:
			if args.game_filter == None or video['game'] == args.game_filter:
				process_single_video( video, youtube_uploader, args )
			else:
				print('Skipping', video['id'], 'because it does not match game.')
	elif args.upload_type == 'video':
		video = get_video( args.destination_id )
		process_single_video( video, youtube_uploader, args )
