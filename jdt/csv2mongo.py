#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Alan Viars

import csv
import sys
import traceback
from collections import OrderedDict
from pymongo import MongoClient


def csv2mongo(csvfile, database_name, collection_name, delete_collection_before_import, host, port):
    """Return a response_dict with summary of csv2mongo transaction."""

    response_dict = OrderedDict()

    try:

        mc = MongoClient(host=host, port=port)
        db = mc[database_name]
        collection = db[collection_name]

        if delete_collection_before_import:
            db.drop_collection(collection)

        # open the csv file.
        csvhandle = csv.reader(open(csvfile, 'r'), delimiter=',')

        rowindex = 0
        mongoindex = 0
        error_list = []

        for row in csvhandle:

            if rowindex == 0:
                column_headers = row
                cleaned_headers = []
                for c in column_headers:
                    c = c.replace(".", "")
                    c = c.replace("(", "")
                    c = c.replace(")", "")
                    c = c.replace("$", "-")
                    c = c.replace(" ", "_")
                    cleaned_headers.append(c)
            else:

                record = OrderedDict(zip(cleaned_headers, row))
                # for k,v in flat_record.items():
                #            if v:
                #                kwargs[k]=v

                try:
                    myobjectid = collection.insert(record)
                    mongoindex += 1

                except:
                    error_message = "Error on row " + \
                        str(rowindex) + ". " + str(sys.exc_info())
                    error_list.append(error_message)

            rowindex += 1

        if error_list:
            response_dict['num_rows_imported'] = rowindex
            response_dict['num_rows_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400
            response_dict['message'] = "Completed with errors"
        else:

            response_dict['num_rows_imported'] = mongoindex
            response_dict['num_csv_rows'] = rowindex
            response_dict['code'] = 200
            response_dict['message'] = "Completed."

    except:
        response_dict['code'] = 500
        response_dict['errors'] = [traceback.print_exc()]

    return response_dict
