# Created by: Andrew Chabot
# CSCI-620 Group Project Phase 1
import psycopg2 as dbpg
import pandas as pd
import io
import time


def connect():
    return dbpg.connect(
        host="127.0.0.1",
        database="soundcloud",
        user="postgres",
        password="csci620soundcloud")


def loadtrackdata(cur):
    # load our data from files into a data frame using pandas
    trackData = pd.read_json(r"smallsc", lines=True)
    """columns=['artwork_url', 'attachments_uri', 'bpm', 'comment_count', 'commentable',
             'created_at', "description", "download_count", "downloadable", "duration",
             "embeddable_by", "favoritings_count", "genre", "id", "isrc", "key_signature",
             "kind", "label_id", "label_name", "last_modified", "license", "likes_count",
             "monetization_model", "original_content_size", "original_format", "permalink",
             "permalink_url", "playback_count", "policy", "purchase_title", "purchase_url",
             "release", "release_day", "release_month", "release_year", "reposts_count", 
             "retrieved_utc", "sharing", "state", "stream_url", "streamable", "tag_list",
             "title", "track_type", "uri", "user":{"avatar_url":"https://i1.sndcdn.com/avatars-000496680540-ri6jvd-large.jpg","id":87506920,"kind":"user","last_modified":"2018/08/29 15:48:15 +0000","permalink":"jackmarlowwzzz","permalink_url":"http://soundcloud.com/jackmarlowwzzz","uri":"https://api.soundcloud.com/users/87506920","username":"JACK MARLOW"},"user_id":87506920,"video_url":null,"waveform_url":"https://w1.sndcdn.com/duWItsfVnYGS_m.png"])
    """
    genreDict = {}
    licenceDict = {}
    kindDict = {}
    tagDict = {}
    userDict = {}

    # create a string io object that we will filter our data into
    track_file = io.StringIO()
    genre_file = io.StringIO()
    license_file = io.StringIO()
    kind_file = io.StringIO()
    tag_file = io.StringIO()
    user_file = io.StringIO()
    track_tag_file = io.StringIO()

    for row in trackData.itertuples():

        currCount = 0
        if row.genre not in genreDict:
            genreDict[row.genre] = currCount
            currCount += 1
            genre_file.write('|'.join(map(str, [currCount, row.genre])) + '\n')
        currCount = 0
        if row.license not in licenceDict:
            licenceDict[row.license] = currCount
            license_file.write('|'.join(map(str, [currCount, row.license])) + '\n')
            currCount += 1
        currCount = 0
        if row.kind not in kindDict:
            kindDict[row.kind] = currCount
            currCount += 1
            kind_file.write('|'.join(map(str, [currCount, row.kind])) + '\n')
        currCount = 0

        for tag in row.tag_list.split(' '):
            if tag not in tagDict:
                tagDict[tag] = currCount
                currCount += 1
                tag_file.write('|'.join(map(str, [currCount, tag])) + '\n')
        currCount = 0

        if row.user['id'] not in userDict:
            user_file.write('|'.join(map(str, [row.user['id'], row.user['username'], row.user['kind'],
                                               row.user['last_modified'], row.user['permalink'], row.user['uri']
                                               ])) + '\n')
            userDict[row.user['id']] = currCount
            currCount += 1

    for row in trackData.itertuples():
        # write to our stringIO object in a separated value format
        for tag in row.tag_list.split(' '):
            track_tag_file.write('|'.join(map(str, [row.id, tagDict[tag]])) + '\n')

        track_file.write('|'.join(map(str, [row.id, row.title, row.uri, row.isrc, row.genre, row.kind, row.license,
                                            row.likes_count, row.commentable, row.comment_count, row.downloadable,
                                            row.download_count, row.created_at, row.description, row.duration,
                                            row.label_name, row.last_modified, row.original_content_size,
                                            row.original_format, row.permalink, row.permalink_url, row.playback_count,
                                            row.retrieved_utc, row.stream_url, row.streamable, row.track_type,
                                            row.waveform_url])) + '\n')

    track_file.seek(0)
    license_file.seek(0)
    genre_file.seek(0)
    tag_file.seek(0)
    kind_file.seek(0)
    user_file.seek(0)
    track_tag_file.seek(0)
    # actually execute our copy statement with the created file"""

    cur.copy_from(license_file, 'licence', sep='|')
    cur.copy_from(genre_file, 'genre', sep='|')
    cur.copy_from(tag_file, 'tag', sep='|')
    cur.copy_from(kind_file, 'kind', sep='|')
    cur.copy_from(user_file, 'user', sep='|')
    cur.copy_from(track_file, 'track', sep='|')
    cur.copy_from(track_tag_file, 'track_label', sep='|')


def loadalldata(conn):
    startTime = time.time()
    try:
        cur = conn.cursor()

        # run all of our individual data load functions and time all of them, printing out the time
        loadtrackdata(cur)
        timeSplit = time.time()
        print("Track load execution time: " + str(timeSplit - startTime) + " seconds")

        # commit our changes to the database and print out the total time the data load took
        cur.close()
        conn.commit()
        timeSplit = time.time()
        print("Total load execution time: " + str(timeSplit - startTime) + " seconds")

    except (Exception, dbpg.DatabaseError) as error:
        # catch any errors(hopefully none) and print them out if they occur
        print(error)
    finally:
        # close our connection
        if conn is not None:
            conn.close()
    conn.close()


if __name__ == '__main__':
    # run it!
    loadalldata(connect())
