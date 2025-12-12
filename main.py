import logging
import argparse
import time
import sqlite3
import os
import hashlib
import datetime
import mediameta # type: ignore
import ffmpeg # type: ignore
import re
import json

from PIL import Image # type: ignore
from PIL.ExifTags import TAGS # type: ignore

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="")

parser.add_argument("-d", "--directory", help="Path to the directory containing the media to be analyzed", type=str, default="../test_directory/")
parser.add_argument("-p", "--process", help="Selecting this option will query the db for any files that have not been processed and process them", action="store_true")
parser.add_argument("-l", "--logging", help="Includes all logging except DEBUG", action="store_true")
parser.add_argument("-f", "--find", help="Find files in the provided path or if one is not provided, in the default path", action="store_true")
parser.add_argument("-v", "--verbose", help="Includes all levels", action="store_true")
parser.add_argument("-i", "--includehidden", help="Ignore hidden files beginning with", action="store_true")
parser.add_argument("-c", "--cleardb", help="Drop all tables from db and recreated them", action="store_true")
parser.add_argument("-n", "--numbertoprocess", help="Specify the number of files to process. If this is omitted, all files will be processed", type=int, default=-1)
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
                        fileDateTime text,
                        latitude text,
                        longitude text,
                        hash text,
                        cameraModel text,
                        cameraMake text,
                        exifDateTime text,
                        moved integer default 0,
                        processed integer default 0
                    )
                """
            }
        ]
    }
    logger.info("SetupDB() - Setting up database")

    connection = sqlite3.connect(DB_INFO["name"])
    connection.row_factory = sqlite3.Row

    cursor = connection.cursor()

    logger.info("SetupDB() - Checking to see if tables exist")

    try:
        for table in DB_INFO["tables"]:

            logger.debug(f"SetupDB() - Checking if \'{table['name']}\' table exists")

            if args.cleardb:
                logger.debug(f"SetupDB() - Dropping table: {table['name']}")
                cursor.execute(f"DROP TABLE IF EXISTS {table['name']}")
                connection.commit()

            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table['name']}'")
            res = cursor.fetchone()

            if res:
                logger.debug(f"SetupDB() - \t\'{table['name']}\' exists!")
            else:
                logger.debug(f"SetupDB() - \t\'{table['name']}\' does not exist!")
                logger.debug(f"SetupDB() - Creating table: '{table['name']}\'")
                try:
                    cursor.execute(table["create_command"])
                    connection.commit()
                    logger.debug(f"SetupDB() - Table: \'{table['name']}\' created successfully!")
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
        
        filetypeReference = {row["extension"] : {"id": row['id'], 'shouldProcess': row['shouldProcess']} for row in res}
        

        return filetypeReference

    except Exception as E:

        raise Exception(f"LOAD KNOWN FILETYPES - {e}")

def addFilesToDB(cursor, files, ftReference):

    logger.info(f"AddFilesToDB - Adding {len(files)} files to db")
    # print(files)
    for file in files:

        logger.debug(f"AddFilesToDB() - Adding \'{file.split('/')[-1]}\' to db")
        
        try:
            # sql = "INSERT INTO media(name, filepath_original, fqdn) VALUES(?, ?, ?)"
            fileInfo = {
                "name": file.split('/')[-1],
                "filepath_original": file[0:file.find(file.split("/")[-1])],
                "fqdn": file,
                "filetypeId": ftReference[file.split(".")[-1].lower()]['id']
            }
            # print(fileInfo)
            cursor.execute("INSERT INTO media(name, filepath_original, fqdn, filetypeId) VALUES(:name,:filepath_original,:fqdn, :filetypeId)", fileInfo)
            cursor.connection.commit()
        except KeyError as e:
            logger.error(f"AddFilesToDB() - The following key does not exist in the filtype reference dict: {e}")
        except Exception as e:
            # print(type(e).__name__)
            if type(e).__name__ == "IntegrityError" and args.verbose:
                logger.error(f"Unable to add file {file} to db. It may already exist")
            if type(e).__name__ != "IntegrityError":
                logger.error(f"AddFilesToDB() - {e}")
        # cursor.execute("INSERT INTO media()")

def findFiles(c):

    try:
        filesToProcess = []
        directories = []
        logger.info(f"FindFiles() - Ssearching for media starting in '{args.directory}'")
        for root, dirs, files in os.walk(args.directory):
            directories.extend([os.path.join(root, d) for d in dirs])

            if len(files):
                for file in files:
                    logger.debug(f"FindFiles() - \tFile: {file}")
                    # print(args.includehidden)
                    
                    if file[0] == "." and not args.includehidden:
                        continue
                    else:
                        logger.debug(f"FindFiles() - \tFile: {os.path.join(root, file)}")
                        filesToProcess.append(os.path.join(root, file))

        logger.info(f"FindFiles() - There were {len(filesToProcess)} files found in {len(directories)} directories")
        
        logger.debug(f"FindFiles - Directories:")
        for d in directories:
            logger.debug(f"FindFiles - \t{d}")

        logger.info("FindFiles() - Getting list of unique filetypes")
        filetypes = {file.split(".")[-1].lower() for file in filesToProcess}
        logger.info(f"FindFiles() - There are {len(filetypes)} unique filetypes in the list of found files")

        logger.debug(f"FindFiles() - Filetypes:")
        for ft in list(filetypes):
            logger.debug(f"FindFiles() - \t{ft}")

        #add new filetypes to DB
        try:
            updateFiletypes(c, list(filetypes))
        except Exception as e:
            logger.error(f"FIND FILES - {e}")

        return filesToProcess

    except Exception as e:
        raise Exception(f"FIND FILES - {e}")

def updateFiletypes(c, ft):

    logger.info("UpdateFiletypes() - Updating filetypes in db")
    for filetype in ft:
        logger.debug(f"updateFiletypesAndRefresh() - Adding \'{filetype}\' to the filetypes table")
        try:        
            c.execute(f'INSERT INTO filetypes ("extension") VALUES ("{filetype}")')
            c.connection.commit()

        except Exception as e:
            logger.error(f"UPDATE FILETYPES AND REFRESH - Unable to add \'{filetype}\' to db - {e}")

def extractGPSData(filepath, gps_exif):

    gps_data = {
        "latitude": "",
        "longitude": "",
        "altitude": ""
    }

    try:
        
        gps_data["longitude"] = f"{round(float(gps_exif[4][0]) + (float(gps_exif[4][1])/60) + (float(gps_exif[4][2])/3600), 6)} {gps_exif[3]}"

    except:

        logger.error(f"Failed to get longitude for img: {filepath}")

    try:

        gps_data["latitude"] = f"{round(float(gps_exif[2][0]) + (float(gps_exif[2][1])/60) + (float(gps_exif[2][2])/3600), 6)} {gps_exif[1]}"

    except:

        logger.error(f"Failed to get latitude for img: {filepath}")

    try:

        gps_data["altitude"] = gps_exif[6]

    except:

        logger.error(f"Failed to get altitude for img: {filepath}")

    return gps_data

def processImage(file, img_data):

    return_info = img_data.copy()

    with Image.open(file["fqdn"]) as img:
        logger.info(f"ProcessImage() - Opened image: {file['fqdn']} for processing")

        try:
            return_info["hash"] = hashlib.md5(img.tobytes()).hexdigest()
            if hasattr(img, '_getexif') and img._getexif():
                exif_data = img._getexif()
                # for k,v in exif_data.items():
                #     print(f"{k} - {TAGS[k]}")
                # Parse GPS data
                try:
                    gps_data = extractGPSData(file["fqdn"], exif_data[34853])
                    return_info.update(gps_data)
                except Exception as e:
                    logger.error(f"GPS Data not found for file: {file['fqdn']}")
                try:
                    return_info["cameraModel"] = f"{exif_data[272]}"
                    return_info["cameraMake"] = f"{exif_data[271]}"
                except Exception as e:
                    logger.error(F"ProcessImage() - Failed to get exif camera model")
                try:
                    return_info["exifDateTime"] = exif_data[36867]
                except Exception as e:
                    logger.error(F"ProcessImage() - Failed to get exif datetime")
                
            return_info['processed'] = 1
            logger.debug(f"ProcessImage() - EXIF Data:")
            for k, v in return_info.items():
                logger.debug(f"ProcessImage() - \t{k}: \t{v}")

            
        except Exception as e:
            raise Exception(f"ProcessImage() - {e}")
        
    return return_info

def processVideo(file_data):

    #Get the number of frames
    #get the width, height
    #Get the duration_ts, duration, nb_frames
    return_data = file_data.copy()

    video_tags = ['width', 'height', 'duration_ts', 'duration', 'nb_frames']
    video_data_dict = {}
    logger.info(f"ProcessingVideo() - Processing file: {file_data["fqdn"]}")
    try:
        # print(f"loading data for: {file_data["fqdn"]}\n")
        probe = ffmpeg.probe(file_data["fqdn"])
        # print(json.dumps(probe, indent=4))clea

        for stream in probe['streams']:
            # print(f"\n{stream['codec_type']}")
            # print(type(set(stream.keys())))
            # print(set(stream.keys()))
            if set(video_tags).issubset(set(stream.keys())):
                for key in video_tags:
                    try:
                        video_data_dict[key] = stream[key]
                    except Exception as e:
                        video_data_dict[key] = ""
                        print(f"Unable to get data for key: {key} - {e}")
                
                return_data['video_data'] = video_data_dict
            else:
                # print("Keys not found")
                pass
        
        try:
            #get make
            tags = probe['format']['tags']
            
            return_data["cameraMake"] = tags['com.apple.quicktime.make']
            return_data["cameraModel"] = tags['com.apple.quicktime.model']
            return_data["exifDateTime"] = tags['com.apple.quicktime.creationdate']

            print(f'TYPE: {type(return_data["exifDateTime"])}')
            pattern = r"[+-]\d+.\d+"
            matches = re.findall(pattern, tags['com.apple.quicktime.location.ISO6709'])
            
            # parsed_location
            return_data["latitude"] = f'{matches[0][1:]} N'
            return_data["longitude"] = f'{matches[1][1:]} W'
            return_data["altitude"] = matches[2]
            
            return_data['video_data']['filesize'] = probe['format']['size']
            return_data['hash'] = hashlib.md5(json.dumps(return_data['video_data']).encode('utf-8')).hexdigest()

        except Exception as e:
            print(f"PROCESSVIDEO() - Faield to get exif data - {e}")
            logger.error(f"PROCESSVIDEO() - Faield to get exif data - {e}")

    except Exception as e:
        
        logger.error("PROCESSVIDEO() - Failed to process video - {e}")

    # print("\n")
    return return_data
    
def processMedia(files, cursor, ftRef):

    logger.info(f"ProcessMedia() - Processing all found files")
    ACCEPTED_FILETYPES = ["jpg", "jpeg", "mov", "m4v"]

    for file in files:
        # print("######\n")
        # print(file["fqdn"])
        img_data = {
            "id": file['id'],
            "hash": "",
            "size": os.path.getsize(file["fqdn"]),
            "latitude": "",
            "longitude": "",
            "altitude": "",
            "processed": 0,
            "filetypeId": ftRef[file["fqdn"].split(".")[-1].lower()]['id'],
            "filepath_original": file["fqdn"],
            "name": file["fqdn"].split("/")[-1],
            "cameraModel": "",
            "cameraMake": "",
            "fileDateTime": datetime.datetime.fromtimestamp(os.path.getctime(file["fqdn"])),
            "exifDateTime": "",
            "fqdn": file["fqdn"]
        }

        logger.info(f"ProcessMedia() - processing: {file['fqdn']}")
            

        try:
            if file['extension'] in ACCEPTED_FILETYPES:

                if file['extension'] in ['jpg', 'jpeg']:
                    try:
                        img_data = processImage(file, img_data)

                        # print(img_data)
                    except Exception as e:
                        logger.error(f"ProcessMedia - {e}")
                
                elif file['extension'] in ['mov', 'm4v']:

                    try:
                        img_data = processVideo(img_data)

                        # print("#######")
                        # print(img_data)

                    except Exception as e:

                        logger.error(f"ProcessMedia - {e}")

            updateFileInDb(cursor, img_data)
        except Exception as e:
            logger.error(f"Failed to process file: {file['fqdn']} - {e}")

def updateFileInDb(cursor, img_data):

    logger.info(f"UpdateFileInDb() - updating file in db")
    
    try:
    
        cursor.execute("UPDATE media SET hash=:hash, size=:size, latitude=:latitude, longitude=:longitude, processed=:processed, fileDateTime=:fileDateTime, exifDateTime=:exifDateTime, cameraMake=:cameraMake, cameraModel=:cameraModel WHERE id=:id", img_data)
        cursor.connection.commit()

    except Exception as e:
        logger.error(f"UPDATEFULEINDB() - {e}")

def getFilesFromDB(cur):

    logger.info(f"GetFilesFromDB() - Retrieving unprocessed files from db")
    cur.execute("SELECT * FROM media LEFT JOIN filetypes ON media.filetypeId = filetypes.id WHERE processed = 0")
    return cur.fetchall()

def main():

    try:
        connection, cursor = setupDB()

        if args.find:
            files = findFiles(cursor)
        
            files = files[0:args.numbertoprocess]

            filetypes = loadKnownFiletypes(cursor)
            #Create a reference dict so that the next step can use the ids of the filetypes
            
        
            # print(filetypeReference)
            addFilesToDB(cursor, files, filetypes)
        if args.process:
            
            filetypes = loadKnownFiletypes(cursor)
            
            #Get all files from the db
            unprocessedFiles = getFilesFromDB(cursor)
            logger.info(f"There are {len(unprocessedFiles)} files that need to be processed")
            processMedia(unprocessedFiles, cursor, filetypes)
            # logger.info("Main() - Known filetypes:")
            # for ft in filetypes:
            #     logger.info(f"Main() - \t{ft[1]}")

    except Exception as e:
        connection.close()
        raise Exception(f"MAIN() - {e}")

    #close the DB connection
    connection.close()

if __name__ == "__main__":

    #record the start time
    startTime = time.perf_counter()

    #set up logger
    fmt = '[%(levelname)s]\t%(asctime)s - %(filename)s:%(lineno)d - %(message)s'
    if args.logging:
        logging.basicConfig(level=logging.INFO, format=fmt)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG, format=fmt)
    else:
        logging.basicConfig(level=100)

    try:
        main()
    except Exception as e:
        logger.error(e)

    #record the end time
    endTime = time.perf_counter()

    logger.info(f'Elapsed time {round(endTime - startTime, 4)} sseconds')