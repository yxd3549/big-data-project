# Created by: Andrew Chabot, Yancarlos Diaz, Daniel Moore, and Jian He
# CSCI-620 Group 2 Phase 1
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

# function to create
def createtables(cur):
    # first drop any of our tables if they already exist
    cur.execute("DROP TABLE IF EXISTS \"user\" CASCADE;")
    cur.execute("DROP TABLE IF EXISTS tag CASCADE;")
    cur.execute("DROP TABLE IF EXISTS genre CASCADE;")
    cur.execute("DROP TABLE IF EXISTS kind CASCADE;")
    cur.execute("DROP TABLE IF EXISTS license CASCADE;")
    cur.execute("DROP TABLE IF EXISTS track CASCADE;")
    cur.execute("DROP TABLE IF EXISTS track_tag CASCADE;")

    # create the table to represent users
    cur.execute("CREATE TABLE \"user\"(\
                    id int,\
                    username varchar(200),\
                    kind varchar(100),\
                    last_modified varchar(100),\
                    permalink varchar(200),\
                    uri varchar(200),\
                    PRIMARY KEY (id)\
                );")

    # create a table to map tags to tag ids
    cur.execute("CREATE TABLE tag(\
                    id int,\
                    tag varchar(100),\
                    PRIMARY KEY (id)\
                );")

    # create a table to map genres to genre ids
    cur.execute("CREATE TABLE genre(\
                    id int,\
                    genre varchar(300),\
                    PRIMARY KEY (id)\
                );")

    # create a table to map kinds to kind ids
    cur.execute("CREATE TABLE kind(\
                    id int,\
                    kind varchar(100),\
                    PRIMARY KEY (id)\
                );")

    # create a table to map licenses to license ids
    cur.execute("CREATE TABLE license(\
                    id int,\
                    license varchar(100),\
                    PRIMARY KEY (id)\
                );")

    # create a table to represent tracks in our database
    cur.execute("CREATE TABLE track(\
                    id int,\
                    title varchar(200),\
                    uri varchar(200),\
                    isrc varchar(100),\
                    genre int,\
                    kind  int,\
                    license int,\
                    likes_count int,\
                    commentable bool,\
                    comment_count int,\
                    downloadable bool,\
                    download_count int,\
                    created_at varchar(100),\
                    description varchar(1000),\
                    duration int,\
                    label_name varchar(100),\
                    last_modified varchar(100),\
                    original_content_size int,\
                    original_format varchar(20),\
                    permalink varchar(200),\
                    permalink_url varchar(500),\
                    playback_count int,\
                    retrieved_utc int,\
                    stream_url varchar(500),\
                    streamable bool,\
                    track_type varchar(100),\
                    waveform_url varchar(200),\
                    PRIMARY KEY (id),\
                    FOREIGN KEY (genre) references genre(id),\
                    FOREIGN KEY (kind) references kind(id),\
                    FOREIGN KEY (license) references license(id)\
                );")

    # create a table to represent the relationship between songs and their tags
    cur.execute("CREATE TABLE track_tag(\
                    track int,\
                    tag int\
                );")

def loadscdata(conn, cur):
    # load our data from our soundcloud file into a data frame using pandas
    trackData = pd.read_json(r"SoundCloud_Tracks_2018-12", lines=True)

    # filter our data to get rid of garbage escape characters that we don't want in output
    trackData = trackData.replace('\|', '/', regex=True)
    trackData = trackData.replace('[\\n\\r]+', '', regex=True)
    trackData = trackData.replace('\\r', '', regex=True)
    trackData = trackData.replace('\\n', '', regex=True)
    trackData = trackData.replace('\r', '', regex=True)
    trackData = trackData.replace('\n', '', regex=True)
    trackData = trackData.replace('\0', '', regex=True)
    trackData = trackData.replace('\\\\', '!', regex=True)

    # initialize our dictionaries that will be used to generate and reference IDs
    genreDict = {}
    licenseDict = {}
    kindDict = {}
    tagDict = {}
    userDict = {}

    # create a string io object that we will filter our data into for each table
    track_file = io.StringIO()
    genre_file = io.StringIO()
    license_file = io.StringIO()
    kind_file = io.StringIO()
    tag_file = io.StringIO()
    user_file = io.StringIO()
    track_tag_file = io.StringIO()

    # initialize our counts that will be used for counting IDs
    genreCount = 0
    licenseCount = 0
    kindCount = 0
    tagCount = 0
    userCount = 0
    skipCount = 0

    # first run through of the data
    print("ready to start copying")
    for row in trackData.itertuples():
        try:
            # filter out garbage escape characters from our user data
            userData = pd.Series(row.user)
            userData = userData.replace('\|', '/', regex=True)
            userData = userData.replace('[\\n\\r]+', ' ', regex=True)
            userData = userData.replace('\\r', ' ', regex=True)
            userData = userData.replace('\\n', ' ', regex=True)
            userData = userData.replace('\r', ' ', regex=True)
            userData = userData.replace('\n', ' ', regex=True)
            userData = userData.replace('\0', '', regex=True)
            userData = userData.replace('\\\\', '!', regex=True)

            # add items to our genre dictionary and write these to our genre file
            if row.genre not in genreDict:
                genreClean = row.genre
                if len(row.genre) > 100:
                    genreClean = row.genre[0:99]
                genreCount += 1
                genreDict[genreClean] = genreCount
                genre_file.write('|'.join(map(str, [genreCount, genreClean])) + '\n')

            # add items to our license dictionary and write these to our license file
            if row.license not in licenseDict:
                licenseClean = row.license
                if len(row.license) > 100:
                    licenseClean = row.license[0:99]
                licenseCount += 1
                licenseDict[licenseClean] = licenseCount
                license_file.write('|'.join(map(str, [licenseCount, licenseClean])) + '\n')

            # add items to our kind dictionary and write these to our kind file
            if row.kind not in kindDict:
                kindClean = row.kind
                if len(row.kind) > 100:
                    kindClean = row.kind[0:99]
                kindCount += 1
                kindDict[kindClean] = kindCount
                kind_file.write('|'.join(map(str, [kindCount, kindClean])) + '\n')

            # add all tags to our tag dictionary if not already there and write these to our tag file
            for tag in row.tag_list.split(' '):
                if len(tag) > 100:
                    tag = tag[0:99]
                if tag not in tagDict:
                    tagCount += 1
                    tagDict[tag] = tagCount
                    tag_file.write('|'.join(map(str, [tagCount, tag])) + '\n')

            # look at our user data for the current row
            if userData.id not in userDict:
                userCount += 1

                # clean more of the input to make sure there are no issues later on
                cleanUsername = str(userData.username)
                if len(userData.username) > 200:
                    cleanUsername = cleanUsername[0:199]
                cleanKind = str(userData.kind)
                if len(userData.kind) > 100:
                    cleanKind = cleanKind[0:99]
                cleanPermalink = str(userData.permalink)
                if len(userData.permalink) > 200:
                    cleanPermalink = cleanPermalink[0:199]
                cleanUri = str(userData.uri)
                if len(userData.uri) > 200:
                    cleanUri = cleanUri[0:199]
                cleanLastMod = str(userData.last_modified)
                if len(cleanLastMod) > 100:
                    cleanLastMod = cleanLastMod[0:99]

                # actually write to the user file
                user_file.write('|'.join(map(str, [userData.id, cleanUsername, cleanKind, cleanLastMod,
                                                   cleanPermalink, cleanUri])) + '\n')
                userDict[userData.id] = userCount
        except Exception:
            # catch any issues and skip addition of the row
            skipCount += 1
            if skipCount % 1000 == 0:
                print('skipped row ' + str(skipCount))
            continue

    print("track copying now")
    skipCount = 0
    for row in trackData.itertuples():

        try:
            # write all of our tags for this track to the track_tag file
            for tag in row.tag_list.split(' '):
                if len(tag) > 100:
                    tag = tag[0:99]
                track_tag_file.write('|'.join(map(str, [row.id, tagDict[tag]])) + '\n')

            # a ton of value cleaning to make sure our copy doesn't fail later
            cleanGenre = str(row.genre)
            if len(cleanGenre) > 100:
                cleanGenre = row.genre[0:99]
            cleanLicense = str(row.license)
            if len(cleanLicense) > 100:
                cleanLicense = cleanLicense[0:99]
            cleanKind = str(row.kind)
            if len(cleanKind) > 100:
                cleanKind = cleanKind[0:99]
            cleanTitle = str(row.title)
            if len(cleanTitle) > 200:
                cleanTitle = cleanTitle[0:199]
            cleanUri = str(row.uri)
            if len(cleanUri) > 200:
                cleanUri = cleanUri[0:199]
            cleanIsrc = str(row.isrc)
            if len(cleanIsrc) > 100:
                cleanIsrc = cleanIsrc[0:99]
            cleanDescription = str(row.description)
            if len(cleanDescription) > 1000:
                cleanDescription = cleanDescription[0:999]
            cleanLabelName = str(row.label_name)
            if len(cleanLabelName) > 100:
                cleanLabelName = cleanLabelName[0:99]
            cleanOrigFormat = str(row.original_format)
            if len(cleanOrigFormat) > 20:
                cleanOrigFormat = cleanOrigFormat[0:19]
            cleanPermalink = str(row.permalink)
            if len(cleanPermalink) > 200:
                cleanPermalink = cleanPermalink[0:199]
            cleanPermalinkUrl = str(row.permalink_url)
            if len(cleanPermalinkUrl) > 500:
                cleanPermalinkUrl = cleanPermalinkUrl[0:499]
            cleanStreamUrl = str(row.stream_url)
            if len(cleanStreamUrl) > 500:
                cleanStreamUrl = cleanStreamUrl[0:499]
            cleanTrackType = str(row.track_type)
            if len(cleanTrackType) > 100:
                cleanTrackType = cleanTrackType[0:99]
            cleanWaveformUrl = str(row.waveform_url)
            if len(cleanWaveformUrl) > 100:
                cleanWaveformUrl = cleanWaveformUrl[0:99]
            cleanLastMod = str(row.last_modified)
            if len(cleanLastMod) > 100:
                cleanLastMod = cleanLastMod[0:99]
            cleanCreatedAt = str(row.created_at)
            if len(cleanCreatedAt) > 100:
                cleanCreatedAt = cleanCreatedAt[0:99]

            # write to our stringIO object in a separated value format
            track_file.write('|'.join(map(str, [int(row.id), cleanTitle, cleanUri, cleanIsrc, genreDict[cleanGenre],
                                                kindDict[cleanKind], licenseDict[cleanLicense], int(row.likes_count),
                                                bool(row.commentable), int(row.comment_count), bool(row.downloadable),
                                                int(row.download_count), cleanCreatedAt, cleanDescription,
                                                int(row.duration), cleanLabelName, cleanLastMod,
                                                int(row.original_content_size), cleanOrigFormat, cleanPermalink,
                                                cleanPermalinkUrl, int(row.playback_count), int(row.retrieved_utc),
                                                cleanStreamUrl, bool(row.streamable), cleanTrackType,
                                                cleanWaveformUrl])) + '\n')
        except Exception:
            # catch any issues and just skip problematic lines
            skipCount += 1
            if skipCount % 1000 == 0:
                print('skipped row ' + str(skipCount))
            continue

    track_file.seek(0)
    license_file.seek(0)
    genre_file.seek(0)
    tag_file.seek(0)
    kind_file.seek(0)
    user_file.seek(0)
    track_tag_file.seek(0)

    # actually execute our copy statements with the created file
    cur.copy_from(license_file, 'license', sep='|')
    print("license table copied")
    cur.copy_from(genre_file, 'genre', sep='|')
    print("genre table copied")
    cur.copy_from(tag_file, 'tag', sep='|')
    print("tag table copied")
    cur.copy_from(kind_file, 'kind', sep='|')
    print("kind table copied")
    cur.copy_from(user_file, '\"user\"', sep='|')
    print("user table copied")
    cur.copy_from(track_file, 'track', sep='|')
    print("track table copied")
    cur.copy_from(track_tag_file, 'track_tag', sep='|')
    print("track_tag table copied")

    # commit our changes before we delete and create primary and foreign keys
    conn.commit()

    # delete duplicate tags
    cur.execute("DELETE FROM track_tag a\
                WHERE a.ctid <> (SELECT min(b.ctid)\
                    FROM   track_tag b\
                    WHERE  a.track = b.track AND\
                        a.tag = b.tag);")
    print("duplicate tags deleted")
    # delete invalid tracks
    cur.execute('DELETE FROM track_tag WHERE track NOT IN (SELECT id FROM track)')
    print("invalid tracks deleted")
    # delete invalid tags
    cur.execute('DELETE FROM track_tag WHERE tag NOT IN (SELECT id FROM tag)')
    print("invalid tags deleted")

    # add primary key to tag table now that we have removed duplicates
    cur.execute("ALTER TABLE track_tag ADD PRIMARY KEY (track, tag);")

    # add a foreign key for both tag and track
    cur.execute("ALTER TABLE track_tag ADD CONSTRAINT trackForeign FOREIGN KEY(track) REFERENCES track(id)")
    cur.execute("ALTER TABLE track_tag ADD CONSTRAINT tagForeign FOREIGN KEY(tag) REFERENCES tag(id)")
    print("track_tag table primary and foreign keys added")


def createandloaddata(conn):
    startTime = time.time()
    try:
        cur = conn.cursor()

        # delete previous tables and
        createtables(cur)
        timeSplit = time.time()
        print("Tables created in " + str(timeSplit - startTime) + " seconds")

        # run all of our individual data load functions and time all of them, printing out the time
        loadscdata(conn, cur)
        timeSplit2 = time.time()
        print("Track load execution time: " + str(timeSplit2 - timeSplit) + " seconds")

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
    createandloaddata(connect())
