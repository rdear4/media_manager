# Media Manager

---

## Abstract

Over the last 2 decades, I have been extremely sloppy with backing up my data in general and more specifically my pictures and videos. I have saved directories to external drives, moved those same directories to different drives while also copying the originals to prevent missing any. I've backed everything up to the cloud, then downloaded it all again and backed them up to external drives.

Some variation of these processes over and over again has resulted in duplicate copies of hundreds of thousands of images and videos. An attempt to add them all to iPhoto with the goal of removing duplicates failed so I decided to try to do it myself.

## Features

- Find all files and store them in a database
- Create a unique hash of the image data for image files
- GUI to explore files and display images
- List all duplicates
    - Based off unique hash
- Display files of specific type
- Options
    - Find Only
    - Find and enter in db
    - Clear db and tables
    - Show duplicates
        - Possible a txt file report as alpha version prior to GUI
    - Process found files
        - Get Exif data and has of image data and update entries in DB
    - Move processed images to new directory and upate the DB when complete.


## Pre-requisites

- install PIL
- install ExifTool

## TO DO

- [x] Set up a logger
- [x] Set up argParser
- [x] Find all files
- [x] Find all unique files extensions
- [x] Create db for files
    - [x] Create table for files
    - [x] Create table for filenames
    - [x] Create table for directories
- [x] Add arg to clear db and tables
- [x] Add files to db
- [x] Add filetypes to db
- [x] ~~Add directories to db~~
- [ ] Gather exif data about each supported filetype
- [ ] Update metadata for each supported file in the db
- [ ] Add flag specifically for finding media
- [ ] Add flag for list duplicate file info (total files, count of uniques, etc)