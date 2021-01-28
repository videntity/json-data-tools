#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Alan Viars

import argparse
import json
import sys
from collections import OrderedDict
from pymongo import MongoClient

def jsonFHIRBundle2mongo(jsonfile, database_name,
                 delete_database_before_import,
                 host,
                 port,
                 collection_name_prefix=''):
    
    """Inserts Bundle as separate resources."""
    print("Opening JSON  file:", jsonfile)
    print("NOTE: Depending on your computer/configuration, large files may need to be split.")
    print("""If you get a "Process killed" in AWS, try increasing the machine's RAM/CPU.""")
    print("For very large files, export entry list into their own files or NDJSON.")

    response_dict = OrderedDict()
    mongoindex = 0
    error_list = []

    mc = MongoClient(host=host, port=port)
    db = mc[database_name]

    if delete_database_before_import:
        print("Drop the database...", database_name)
        mc.drop_database(database_name)
    
    print("""Opening JSON file and expecting a FHIR Bundle or at least an object with an "entry" list with more objects..""")
    print("Loading the JSON file into memory...")
    with open(jsonfile) as f:
        
        item = json.loads(f.read(), object_pairs_hook=OrderedDict)
        print("JSON loaded successfully...")
        if not isinstance(item, type(OrderedDict())):
            error = "Not a JSON object."
            error_list.append(error)
            response_dict['num_errors'] = len(error_list)
            print(error)
            response_dict['errors'] = error_list
            response_dict['code'] = 400            
            response_dict['message'] = "Completed with errors."
            return response_dict

        print("Object loaded successfully...")
        
        response_dict['inputfile'] = jsonfile
        response_dict['database'] = database_name
        response_dict['num_entries_imported'] = mongoindex
        response_dict['fhir_resources_included'] = []
        response_dict['num_errors'] = 0
        
        if "entry" not in item.keys():
            error = "Failed to find entry [] list."
            print(error)
            error_list.append(error)
            response_dict['num_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400            
            response_dict['message'] = "Completed with errors."
            return response_dict
        else:
            print("Processing %s items." % (len(item['entry'])))
        i=0
        for e in item['entry']:
            try:
                #print(mongoindex,e['resource']["resourceType"])
                collection_name = "%s%s" % (collection_name_prefix,e['resource']["resourceType"])
                collection = db[collection_name]    
                myobjectid = collection.insert(e)
                mongoindex += 1
                response_dict['num_entries_imported'] = mongoindex
                response_dict['num_errors'] = len(error_list)
            except Exception:
                print(sys.exc_info())
                error_message = "Entry %s failed. - %s" % (e, sys.exc_info())
                error_list.append(error_message)
            i += 1

        response_dict['inputfile'] = jsonfile
        response_dict['database'] = database_name
        response_dict['errors'] = error_list
        response_dict['num_entries_imported'] = mongoindex
        response_dict['num_errors'] = len(error_list)
        
        if response_dict['num_errors']:
            response_dict['code'] = 400
            response_dict['message'] = "Completed with errors."
        else:
            response_dict['code'] = 200
            response_dict['message'] = "OK"
    return response_dict

if __name__ == "__main__":

    # Parse args
    parser = argparse.ArgumentParser(
        description='Input JSON FHIR Bundle file.')
    parser.add_argument(
        dest='input_json_file',
        action='store',
        help='Input the JSON file to load here.')
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
    json_file = args.input_json_file
    database = args.db_name
    delete_database_before_import = args.delete
    collection_name_prefix = args.cnp
    host = args.host
    port = args.port

    result = jsonFHIRBundle2mongo(
        json_file,
        database,
        delete_database_before_import,
        host,
        port,
        collection_name_prefix)

    # output the JSON transaction summary
    print(json.dumps(result, indent=4))