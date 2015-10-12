import argparse
import subprocess
import requests
import os
import io

from youtube import YoutubeUploader

#TODO: switch to youtube-dl again because livestreamer often arborts download too early

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
		'length': json['length'] }

def get_video( id ):
# Return a video in the internal format. The video is specified by its video id according to the Twitch api.
	url = 'https://api.twitch.tv/kraken/videos/{}'.format( id )
	r = requests.get( url, headers=headers_v3 )
	json = r.json()
	return json_to_video( json )


def get_videos( channel_name, last_video=None, legacy_mode=True ):
	#last video is none or the id of a video
	#if it is not none only videos that come before last video are returned
	url = 'https://api.twitch.tv/kraken/channels/{}/videos'.format( channel_name )
	videos_per_page = 100 #100 is the maximum videos we can request per call
	if legacy_mode: hls = 'false'
	else: hls = 'true'
	payload = { 'limit': videos_per_page,
                    'offset': 0,
                    'broadcasts': 'true',
                    'hls': hls }
	def get_page( pagenumber ):
		_payload = payload.copy()
		_payload['offset'] = pagenumber * videos_per_page
		r = requests.get( url, params=_payload, headers=headers_v3)
		json = r.json()
		videos = json['videos']
		result = []
		for v in videos:
			result.append( json_to_video( v ) )
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

def download_video( video, args ):
# TODO: call livestreamer over python module, not command line
# Download a video from Twitch.
# Considerations:
# 	-livestreamer and youtube-dl --hls-prefer-native create the same output.
# 	-youtube-dl without native hls uses ffmpeg for the download.
#	-ffmpeg needs to correct the audio format with -bsf:a aac_adtstoasc which is done by youtube-dl automatically. Otherwise it has to be done manually to split files with ffmpeg.
#	-Vods that have muted sections at the start will appear to contain no audio at all to ffmpeg and youtube (if uploaded in native format).
#	 to work around this ffmpeg needs to look further ahead in the file with -probesize 9000000000000000000 -analyzeduration 9000000000000000000 for example.
#	-Downloading with livestreamer is faster because of the thread option
	if args.file_name:
		filename = args.file_name
	else:
		filename = '{}.mp4'.format( video['id'] )
	if args.file_already_exists: return filename
	#command = [
	#	'youtube-dl',
	#	#'--hls-prefer-native',
	#	'--quiet',
	#	'--no-call-home',
	#	'-o', filename,
	#	video['url'] ]
	command = [
		'livestreamer',
		'--hls-segment-threads', '1', #a higher number provides more download speed but it seems to sometimes cause read timeouts
		'--hls-segment-timeout', '60',
		'--hls-segment-attempts', '5',
		'--stream-segment-attempts', '5',
		#'--quiet',
		'-o', filename,
		video['url'], 'best' ]

	if args.dry_run:
		command.append( '--simulate' )
	print( "Invoking download:", command )
	process = subprocess.Popen( command )
	process.wait()
	if process.returncode != 0:
		print( "Error while executing youtube-dl." )
		raise RuntimeError("Youtube-dl did not return 0.")
	print( "Done downloading", video['id'] )
	return filename

def get_video_title(video, part_number=None):
	title = '{} stream from {}'.format( video['channel_name'], video['recorded_at'] )
	if part_number is not None:
		title += ' part {}'.format( part_number )
	return title

def upload_video( video, filename, args, youtube_uploader, part_number = None ):
	if args.dont_use_default_tags:
		tags = args.tags.split(",")
	else:
		tags = default_tags + args.tags.split(",")
	title = get_video_title(video, part_number)
	description = 'Original title: {}\nOriginal description: {}\nOriginal date: {}\nOriginal Twitch id: {}'.format(
		video['title'],
		video['description'],
		video['recorded_at'], video['id'] ) #not sure if the unicode encode thing is needed

	print("Starting upload")
	youtube_video_id = youtube_uploader.upload(filename, title, description, "20", tags, "private")
	print( "Done uploading", video['id'], "as", youtube_video_id )
	return youtube_video_id

def split_video( filename, video_length, part_length ):
	#All durations are in seconds
	extra_length = 1 #make each part a bit longer to make sure youtube does not loose anything
	print( "Splitting", filename, "every", part_length, "seconds." )
	for current_time in range( 0, video_length, part_length ):
		part_name = str(current_time) + filename
		command = [
			'ffmpeg',
			'-loglevel', 'warning',
			'-i', filename,
			'-bsf:a', 'aac_adtstoasc',
			'-codec', 'copy',
			'-ss', str(current_time), #seek after -i for accuracy
			'-t', str(part_length + extra_length), #ffmpeg will correctly split the lsat part even if the -t paramter wwould be too longV
			part_name ]
		print( "Invoking ffmpeg with command:", command )
		process = subprocess.Popen( command );
		process.wait()
		if process.returncode != 0:
			print( "Error while splitting file." )
			raise RuntimeError("Ffmpeg did not return 0.")
		yield part_name

def process_single_video( video, youtube_uploader, args ):
	filename = download_video( video, args )
	if video['length'] > args.split_at:
		if not args.dont_use_playlist:
			playlist_id = youtube_uploader.create_playlist(get_video_title(video), privacyStatus="public")['id']
		part_number = 1
		for part_name in split_video( filename, video['length']+1, args.split_at ):
			youtube_video_id = upload_video( video, part_name, args, youtube_uploader, part_number )
			if not args.dont_use_playlist:
				youtube_uploader.add_to_playlist(playlist_id, youtube_video_id)
			os.remove( part_name )
			part_number += 1
	else:
		upload_video( video, filename, args )
	if args.state_file:
		with open( args.state_file, 'w' ) as state_file:
			state_file.writelines( [video['id'] + '\n'] )
	if not args.dont_delete_after_upload:
		os.remove( filename )

if __name__ == "__main__":
	parser = argparse.ArgumentParser( description='Automatically upload twitch vods to youtube.' )
	parser.add_argument( '--authentication-file', help='The file used to authenticate with youtube.', required=True )
	parser.add_argument( '--client-secrets-file', help='Youtube developer api client secrets file. Only needed if auth file is not valid anymore.', required=False )
	parser.add_argument( '--state-file', help='The file that contains the state for channel uploads.' )
	parser.add_argument( '--upload-type', choices=['channel', 'video'], help='Upload a whole channel or a single video.', required=True )
	parser.add_argument( '--destination-id', help='Channel or video id of location to be processed.', required=True )
	parser.add_argument( '--file-name', help='Custom file name if processing a single video' )
	parser.add_argument( '--file-already-exists', help='If the video was already downloaded and does not need to be downloaded again.', action='store_true')
	parser.add_argument( '--dont-delete-after-upload', help='Do not clean up the downloaded file after it was successfully uploaded.', action='store_true' )
	parser.add_argument( '--tags', help='Addtional tags for the uploaded videos, comma separated.', default='')
	parser.add_argument( '--dont-use-default-tags', help='Do not add the default tags to the video.', action='store_true' )
	parser.add_argument( '--start-after', help='When in channel mode process only recordings newer than this video id' )
	parser.add_argument( '--split-at', help='Split videos in parts of duration in seconds.', type=int, default=60*60*11 )
	parser.add_argument( '--dry-run', help='Dont download or upload anything.', action='store_true' )
	parser.add_argument( '--twitch-legacy-mode', help='Download only videos whose id start with a b instead of a v. Those videos are not created on twitch anymore but some old ones exist.', action='store_true' )
	parser.add_argument( '--client-id', help='Your twitch application\'s client id', required=True )
	parser.add_argument( '--dont-use-playlist', help='Do not automatically create a playlist for videos that get split in multiple parts.', action='store_true')
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
		videos = get_videos( args.destination_id, start_after, args.twitch_legacy_mode )
		videos.reverse()
		for video in videos:
			process_single_video( video, youtube_uploader, args )
	elif args.upload_type == 'video':
		video = get_video( args.destination_id )
		process_single_video( video, youtube_uploader, args )
