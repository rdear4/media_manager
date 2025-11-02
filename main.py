import logging
import argparse
import time
import sqlite3
import os

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="")

parser.add_argument("-p", "--path", help="Path to the directory containing the media to be analyzed", type=str, default="../test_directory/")
parser.add_argument("-l", "--logging", help="Enables debug and error ", action="store_true")
parser.add_argument("-i", "--includehidden", help="Ignore hidden files beginning with", action="store_true")
parser.add_argument("-c", "--cleardb", help="Drop all tables from db and recreated them", action="store_true")

args = parser.parse_args()


def setupDB():

    DB_INFO = {
        "name": "media.db",
        "tables": [
            {
                "name": "filetypes",
                "create_command": """
                    CREATE TABLE filetypes (
                        id INTEGER PRIMARY KEY,
                        extension text UNIQUE,
                        shouldProcess integer default 1
                    )
                """
            },
            {
                "name": "media",
                "create_command": """
                    CREATE TABLE media (
                        id INTEGER PRIMARY KEY,
                        name text,
                        filetypeId integer,
                        filepath_original text,
                        filepath_current text,
                        fqdn text UNIQUE,
                        size integer,
                        date text,
                        latitude text,
                        longitude text,
                        hash text,
                        cameraModel text,
                        exifDateTime text,
                        processed integer default 0
                    )
                """
            },
            {
                "name": "directories",
                "create_command": """
                    CREATE TABLE directories (
                        id INTEGER PRIMARY KEY,
                        dirname text,
                        dirpath text,
                        filecount int,
                        fully_searched int DEFAULT 0
                    )
                """
            },
        ]
    }
    logger.info("SetupDB() - Setting up database")
    logger.info("SetupDB() - Creating connection")

    connection = sqlite3.connect(DB_INFO["name"])
    cursor = connection.cursor()

    logger.info("SetupDB() - Checking to see if tables exist")

    try:
        for table in DB_INFO["tables"]:

            logger.info(f"SetupDB() - Checking if \'{table['name']}\' table exists")
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table['name']}'")
            res = cursor.fetchone()

            if args.cleardb:
                logger.info(f"SetupDB() - Dropping table: {table['name']}")
                cursor.execute(f"DROP TABLE IF EXISTS {table['name']}")
                connection.commit()

            if res:
                logger.info(f"SetupDB() - \t\'{table['name']}\' exists!")
            else:
                logger.info(f"SetupDB() - \t\'{table['name']}\' does not exist!")
                logger.info(f"SetupDB() - Creating table: '{table['name']}\'")
                try:
                    cursor.execute(table["create_command"])
                    connection.commit()
                    logger.info(f"SetupDB() - Table: \'{table['name']}\' created successfully!")
                except Exception as e:
                    raise Exception(f"Failed to create table: \'{table['name']}\' - {e}")

    except Exception as e:

        raise Exception(f"SETUP DB - {e}")

    return connection, cursor

def loadKnownFiletypes(c):
    logger.info(f"LoadKnownFiletypes - Loading known filetypes from db")
    try:

        c.execute("SELECT * FROM filetypes")
        res = c.fetchall()

        return res

    except Exception as E:

        raise Exception(f"LOAD KNOWN FILETYPES - {e}")

def addFileToDB():

    pass

def findFiles(c):

    try:
        filesToProcess = []
        directories = []
        logger.info(f"FindFiles() - Ssearching for media starting in '{args.path}'")
        for root, dirs, files in os.walk(args.path):
            # logger.info(f"FindFiles() - {len(dirs)} directories were found")
            directories.extend([os.path.join(root, d) for d in dirs])

            if len(files):
                for file in files:
                    # logger.info(f"FindFiles() - \tFile: {file}")
                    # print(args.includehidden)
                    
                    if file[0] == "." and not args.includehidden:
                        continue
                    else:
                        # logger.info(f"FindFiles() - \tFile: {os.path.join(root, file)}")
                        filesToProcess.append(os.path.join(root, file))

        logger.info(f"FindFiles() - There were {len(filesToProcess)} files found in {len(directories)} directories")
        
        logger.info(f"FindFiles - Directories:")
        for d in directories:
            logger.info(f"FindFiles - \t{d}")

        logger.info("FindFiles() - Getting list of unique filetypes")
        filetypes = {file.split(".")[-1].lower() for file in filesToProcess}
        logger.info(f"FindFiles() - There are {len(filetypes)} unique filetypes in the list of found files")

        logger.info(f"FindFiles() - Filetypes:")
        for ft in list(filetypes):
            logger.info(f"FindFiles() - \t{ft}")

        #add new filetypes to DB
        try:
            updateFiletypes(c, list(filetypes))
        except Exception as e:
            logger.error(f"FIND FILES - {e}")

        return filesToProcess

    except Exception as e:
        raise Exception(f"FIND FILES - {e}")

def updateFiletypes(c, ft):


    for filetype in ft:
        logger.info(f"updateFiletypesAndRefresh() - Adding \'{filetype}\' to the filetypes table")
        try:        
            c.execute(f'INSERT INTO filetypes ("extension") VALUES ("{filetype}")')
            c.connection.commit()

        except Exception as e:
            logger.error(f"UPDATE FILETYPES AND REFRESH - Unable to add \'{filetype}\' to db - {e}")
        

def main():

    try:
        connection, cursor = setupDB()

        files = findFiles(cursor)

        filetypes = loadKnownFiletypes(cursor)

        logger.info("Main() - Known filetypes:")
        for ft in filetypes:
            logger.info(f"Main() - \t{ft[1]}")

    except Exception as e:
        connection.close()
        raise Exception(f"MAIN() - {e}")

if __name__ == "__main__":

    #record the start time
    startTime = time.perf_counter()

    #set up logger
    fmt = '[%(levelname)s]\t%(asctime)s - %(message)s'
    if args.logging:

        logging.basicConfig(level=logging.INFO, format=fmt)
    else:
        logging.basicConfig(level=100)

    try:
        main()
    except Exception as e:
        logger.error(e)

    #record the end time
    endTime = time.perf_counter()

    logger.info(f'Elapsed time {round(endTime - startTime, 4)} sseconds')