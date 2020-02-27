jdt - JSON Data Tools
=====================

Version: 0.4.2

This repository contains a handful of command-line utilities and related code libraries for
processing CSV, JSON, and [Newline Delimited JSON].(http://ndjson.org/) files.

It also contains tools for loading then into:


* MongoDB
* [FHIR](https://www.hl7.org/fhir/) servers.

_Please consider a code contribution for imports into CouchDB, Redis, Elastic Search, etc :-)_


The command-line utilities are as folloes:

* `csv2mongo`           - Converting a CSV into documents directly into a MongoDB database/collection.
* `json2mongo`          - Convert a JSON file object into a record into a MongoDB database/collection.
* `jsondir2ndjson`      - Traverse a directory path and convert all JSON documents into a single NDJSON file.  
* `ndjson2mongo`        - Import an NDJSON file into a MongoDB database/collection.
* `ndjsonurl2mongo`     - Import an NDJSON file, at a given URL, into a MongoDB database/collection.
* `ndjson2fhir`         - Import an NDJSON file into a FHIR server using POST/CREATE (experimental).
* `ndjsonurl2fhir`      - Import an NDJSON file, at a given URL, into a FHIR server using POST/CREATE (experimental).
* `sftp-ndjson2mongo`   - Download files from SFTP, then import NDJSON and CSV and import them into MongoDB.  


Installation
------------


You can install the tool using `pip`.

To install with pip just type:


    ~$ sudo pip install jdt

Note: If you use `sudo`, the scripts  will be installed at the
system level and used by all users. Add  `--upgrade` to the above
install instructions to ensure you fetch the newest version.

This library assumes you have MongoDB installed and running. See http://docs.mongodb.org/manual/installation/ for more installation and instructions for your OS.

General Usage
-------------

For info about each command type:
    
    `<command> -h`

The general rule is:

    <command> <file/dir to load> <db_name> <collection_name>

By default, if you pass in db/collection name that already exists in your MongoDB, the import will only add to it. If you pass the `-d` option to a command, the specified collections will be dropped before importing the new data.

Also by default:

    Host = 127.0.0.1
    Port = 27017
    
These options can be changed with the --host and --port options respectively.

csv2mongo
---------

`csv2mongo` convert a CSV into a MongoDB collection.  The script expects the first row of
data to contain header information. Any whitespace and other funky characters in the
header row are auto-fixed by converting to ` `, `_`, or `-`.

Usage:

    ~$ csv2mongo [CSVFILE] [DATABASE] [COLLECTION] 


Example:

    ~$ csv2mongo npidata_20050523-20140413.csv npi_database npi_collection

    {
    "num_rows_imported": 9, 
    "num_csv_rows": 10, 
    "code": 200, 
    "message": "Completed."
    }




json2mongo
----------

`json2mongo` imports a JSON object file into a MongoDB document. The file is checked
for validity (i.e. {}) before attempting to import it into MongoDB.


Usage:

    ~$ json2mongo [JSONFILE] [DATABASE] [COLLECTION] 

Example:


    ~$ json2mongo test.json npi nppes 
    
    {
    "num_rows_imported": 1, 
    "num_file_errors": 0, 
    "code": 200, 
    "message": "Completed without errors."
    }



jsondir2mongo
-------------


`jsondir2mongo` imports a directory containing files of JSON objects to MongoDB documents. It will walk through subdirectories as well, looking for JSON files. The files are checked for validity (i.e. {}) before attempting to import it each into MongoDB. Files that are not JSON objects are automatically skipped.  A summary is returned when the process ends.

Usage:

    ~$ jsondir2mongo [JSONFILE] [DATABASE] [COLLECTION]


Example:


    ~$ json2dirmongo data npi nppes 

Example output:


    Clearing the collection prior to import.

Start the import of the directory data into the collection test within the database csv2json.


    ('Getting a list of all files for importing from', '../provider-data-tools/tests')
    Done creating file list. Begin file import.
    {
        "num_files_attempted": 31, 
        "num_files_imported": 30, 
        "num_file_errors": 1, 
        "errors": [
            "Error writing ../provider-data-tools/tests/json_schema_test.json to Mongo. (<class 'bson.errors.InvalidDocument'>, InvalidDocument(\"key '$schema' must not start with '$'\",), <traceback object at 0x7fe1941fa3b0>)"
        ], 
        "code": 400, 
        "message": "Completed with errors."
    }



In the above example, 30/31 JSON files were imported. The error reported that json_schema_test.json was an invalid JSON document and pointed out why.
