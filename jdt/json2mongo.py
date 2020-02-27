#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4


import json
import sys
from collections import OrderedDict
from pymongo import MongoClient


def json2mongo(jsonfile, database_name, collection_name,
               delete_collection_before_import, host, port):
    """Return a response_dict with a summary of json2mongo transaction."""
    # print "Start the import of", jsonfile, "into the collection",
    # collection_name, "within the database", database_name, "."

    response_dict = OrderedDict()
    fileindex = 0
    mongoindex = 0
    error_list = []

    mc = MongoClient(host=host, port=port)
    db = mc[database_name]
    collection = db[collection_name]

    if delete_collection_before_import:
        db.drop_collection(collection)
    fh = open(jsonfile, 'rU')

    j = fh.read()

    try:
        j = json.loads(j, object_pairs_hook=OrderedDict)

        if not isinstance(j, type(OrderedDict())):
            error_message = "File " + \
                str(jsonfile) + " did not contain a JSON object, i.e. {}."
            error_list.append(error_message)

    except:
        error_message = "File " + \
            str(jsonfile) + " did not contain valid JSON."
        error_list.append(error_message)

    if not error_list:

        myobjectid = collection.insert(j)
        mongoindex += 1

        if error_list:
            response_dict['num_files_imported'] = mongoindex
            response_dict['num_file_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400
            response_dict['message'] = "Completed with errors."

        else:
            response_dict['num_rows_imported'] = mongoindex
            response_dict['num_file_errors'] = len(error_list)
            response_dict['code'] = 200
            response_dict['message'] = "Completed without errors."

    else:
        response_dict['num_rows_imported'] = mongoindex
        response_dict['num_file_errors'] = len(error_list)
        response_dict['code'] = 500
        syserror = sys.exc_info()
        errors = error_list
        if syserror[0]:
            errors.append(str(sys.exc_info()))

        response_dict['errors'] = errors

    return response_dict
