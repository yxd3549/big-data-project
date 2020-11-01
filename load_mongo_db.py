from pymongo import MongoClient
import json

mongodb = MongoClient("mongodb://localhost").soundcloud


def load_data():
    with open('SoundCloud_Tracks_2018-12') as file:
        i = 0
        errors = 0
        tracks_collection = []
        users_collection = {}
        for track in file:
            # Remove escape characters
            track = track.replace('\|', '/')
            track = track.replace('[\\n\\r]+', '')
            track = track.replace('\\r', '')
            track = track.replace('\\n', '')
            track = track.replace('\r', '')
            track = track.replace('\n', '')
            track = track.replace('\0', '')
            track = track.replace('\\\\', '!')

            try:
                track = json.loads(track)
                user_object = track['user']
                user_document = {
                    '_id': user_object['id'],
                    'username': user_object['username'],
                    'kind': user_object['kind'],
                    'last_modified': user_object['last_modified'],
                    'permalink': user_object['permalink'],
                    'uri': user_object['uri']
                }
                # print(user_document)
                track_document = {
                    '_id': track['id'],
                    'title': track['title'],
                    'uri': track['uri'],
                    'isrc': track['isrc'],
                    'genre': track['genre'],
                    'kind': track['kind'],
                    'license': track['license'],
                    'likes_count': track['likes_count'] if 'likes_count' in track else None,
                    'commentable': track['commentable'],
                    'comment_count': track['comment_count'] if 'comment_count' in track else None,
                    'downloadable': track['downloadable'],
                    'download_count': track['download_count'] if 'download_count' in track else None,
                    'created_at': track['created_at'],
                    'description': track['description'],
                    'duration': track['duration'],
                    'label_name': track['label_name'],
                    'last_modified': track['last_modified'],
                    'original_content_size': track['original_content_size'],
                    'original_format': track['original_format'],
                    'permalink': track['permalink'],
                    'permalink_url': track['permalink_url'],
                    'playback_count': track['playback_count'] if 'playback_count' in track else None,
                    'retrieved_utc': track['retrieved_utc'],
                    'stream_url': track['stream_url'],
                    'streamable': track['streamable'],
                    'track_type': track['track_type'],
                    'waveform_url': track['waveform_url'],
                    'tags': track['tag_list'].split(' ')
                }
                tracks_collection.append(track_document)
                users_collection[user_object['id']] = user_document
                i += 1
            except json.JSONDecodeError:
                errors += 1

        mongodb.tracks.insert_many(tracks_collection)
        mongodb.users.insert_many(users_collection.values())
        print('Imported Documents:', i)


if __name__ == '__main__':
    load_data()
