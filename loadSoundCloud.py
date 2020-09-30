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
    nameD = pd.read_csv(r"name.basics.tsv", sep='\t')
    nameData = pd.DataFrame(nameD,
                            columns=['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession',
                                     'knownForTitles'])

    # create a string io object that we will filter our data into
    csv_file = io.StringIO()
    for row in nameData.itertuples():
        # turn our person id into an integer
        cleanPID = row.nconst[2:]

        # write to our stringIO object in a separated value format
        csv_file.write('|'.join(map(str, [cleanPID,
                                          row.primaryName,
                                          row.birthYear,
                                          row.deathYear])) + '\n')

    csv_file.seek(0)
    # actually execute our copy statement with the created file
    cur.copy_from(csv_file, 'member', sep='|')


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
