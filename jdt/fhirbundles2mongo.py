#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Alan Viars

import argparse
import json
import sys
from collections import OrderedDict
from pymongo import MongoClient
import ndjson


def ndjsonFHIRBundle2mongo(ndjsonfile, database_name,
                 delete_database_before_import,
                 host,
                 port,
                 collection_name_prefix=''):
    
    """Inserts Bundle as separate resources."""
    print("Opening NDJSON  file:", ndjsonfile,)
    print("NOTE: Depending on your computer/configuration, large files may need to be split.")
    print("""If you get a "Process killed" in AWS, try A.) splitting the files using "split" or B.) increase the machine's RAM/CPU.""")

    response_dict = OrderedDict()
    fileindex = 0
    mongoindex = 0
    error_list = []

    mc = MongoClient(host=host, port=port)
    db = mc[database_name]

    if delete_database_before_import:
        print("Dropping the database...", database_name)
        mc.drop_database(database_name)
    print("Opening NDJSON file. Expecting FHIR Bundles...")
    with open(ndjsonfile) as f:
        print("Loading the NDJSON into memory...")
        data = ndjson.load(f, object_pairs_hook=OrderedDict)
        print("Data loaded...")
        print("Processing items...")
        i=0
        for item in data:
            # print("item",item)
            i +=1
            try:
                if not isinstance(item, type(OrderedDict())):
                    error_message = "Line %s did not contain a JSON object." % (i)
                    error_list.append(error_message)
                # insert the item/document
                for e in item['entry']:
                    #print(mongoindex,e['resource']["resourceType"])
                    collection_name = "%s%s" % (collection_name_prefix,e['resource']["resourceType"])
                    collection = db[collection_name]    
                    myobjectid = collection.insert(e)
                    mongoindex += 1

            except:
                print(sys.exc_info())
                error_message = "Line %s - %s" % (i, sys.exc_info())
                error_list.append(error_message)

        if error_list:
            response_dict['file'] = ndjsonfile
            response_dict['database'] = database_name
            response_dict['num_files_imported'] = mongoindex
            response_dict['num_file_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400
            response_dict['message'] = "Completed with errors."

        else:
            response_dict['file'] = ndjsonfile
            response_dict['database'] = database_name
            response_dict['num_rows_imported'] = mongoindex
            response_dict['num_file_errors'] = len(error_list)
            response_dict['code'] = 200
            response_dict['message'] = "Completed without errors."

    return response_dict

if __name__ == "__main__":

    # Parse args
    parser = argparse.ArgumentParser(
        description='Input NDJSON file.')
    parser.add_argument(
        dest='input_ndjson_file',
        action='store',
        help='Input the NDJSON file to load here')
    parser.add_argument(
        dest='db_name',
        action='store',
        help="Enter the Database name you want to import the JSON to")
    parser.add_argument('-d', '--delete', dest='delete', action='store_true',
                        help='Delete previous collection upon import')
    parser.add_argument(
        '--host',
        dest='host',
        action='store',
        default='127.0.0.1',
        help='Specify host. Default is 127.0.0.1 ')
    parser.add_argument(
        '-c', '--collection_name_prefix',
        dest='cnp',
        action='store',
        default='',
        help='Specify a prefix for FHIR resource collection names.Default is blank')
    parser.add_argument(
        '-p',
        '--port',
        dest='port',
        action='store',
        default=27017,
        help='Specify port. Default is 27017')
    args = parser.parse_args()
    ndjson_file = args.input_ndjson_file
    database = args.db_name
    delete_database_before_import = args.delete
    collection_name_prefix = args.cnp
    host = args.host
    port = args.port

    result = ndjsonFHIRBundle2mongo(
        ndjson_file,
        database,
        delete_database_before_import,
        host,
        port,
        collection_name_prefix)

    # output the JSON transaction summary
    print(json.dumps(result, indent=4))
