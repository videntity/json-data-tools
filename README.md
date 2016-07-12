jdt - JSON Data Tools
=====================

Version: 0.0.2

This repository contains a handful of command-line utilities and
related code libraries for parsing CSVs into JSON and loading MongoDB.

They are:

* csv2mongo           - Converting a CSV into documents directly into a MongoDB database/collection.
* json2mongo          - Convert a JSON file object into a record in a MongoDB database/collection.
* jsondir2mongo       - Convert a directory of files containing JSON objects into documents in a MongoDB database/collection.

Installation
------------

You can install the tool using `pip`.

To install with pip just type:

    ~$ sudo pip install jdt

Note: If you use `sudo`, the scripts  will be installed at the
system level and used by all users. Add  `--upgrade` to the above
install instructions to ensure you fetch the newest version.



csv2mongo
---------

`csv2mongo` convert a CSV into a MongoDB collection.  The script expects the first row of
data to contain header information. Any whitespace and other funky characters in the
header row are auto-fixed by converting to ` `, `_`, or `-`.

Usage:

    ~$ csv2mongo [CSVFILE] [DATABASE] [COLLECTION] [DELETE_COLLECTION_BEFORE_IMPORT (T/F)] [HOST] [PORT]


Example:

    ~$ csv2mongo npidata_20050523-20140413.csv npi nppes T 127.0.0.1 27017




json2mongo
----------

`json2mongo` imports a JSON object file into a MongoDB document. The file is checked
for validity (i.e. {}) before attempting to import it into MongoDB.


Usage:

    ~$ json2mongo [JSONFILE] [DATABASE] [COLLECTION] [DELETE_COLLECTION_BEFORE_IMPORT (T/F)] [HOST] [PORT]

Example:


    ~$ json2mongo test.json npi nppes T 127.0.0.1 27017



jsondir2mongo
-------------


`jsondir2mongo` imports a directory containing files of JSON objects to MongoDB documents.
 The files are checked for validity (i.e. {}) before attempting to import it each into
 MongoDB. Files that are not JSON objects are automatically skipped.  A summary is returned when the process ends.

Usage:

    ~$ jsondir2mongo [JSONFILE] [DATABASE] [COLLECTION] [DELETE_COLLECTION_BEFORE_IMPORT (T/F)] [HOST] [PORT]


Example:


    ~$ json2dirmongo data npi nppes T 127.0.0.1 27017

Example output:


    Clearing the collection prior to import.

Start the import of the directory data into the collection test within the database csv2json.


    {
            "info": [
                "The collection was cleared prior to import."
            ],
            "num_files_attempted": 4,
            "num_files_imported": 2,
            "num_file_errors": 2,
            "errors": [
                "File data/3.json did not contain a json object, i.e. {}.",
                "File data/4.json did not contain valid JSON."
            ],
            "code": 400,
            "message": "Completed with errors."
        }


In the above example, the files `1.json` and `2.json` were processed while `3.json` and
`4.json` were not imported.
