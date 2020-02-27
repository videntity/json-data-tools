#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Alan Viars

import argparse
import json
import os
import ndjson
import sys
from collections import OrderedDict


def jsondir2ndjson(json_dir, output_filename):
    """Convert all json files under a path into a single ndjson file."""

    fileindex = 0
    error_list = []
    success_index = 0
    response_dict = OrderedDict()
    onlyfiles = []
    try:
        out_fh = open(output_filename, 'w')
        writer = ndjson.writer(out_fh)

        # get the files in the specified directory
        print("Getting a list of all files for importing from", json_dir)
        for root, dirs, files in os.walk(json_dir):
            for file in files:
                if file.endswith(".json") or file.endswith(".js"):
                    onlyfiles.append(os.path.join(root, file))

        for f in onlyfiles:
            j = None
            error_message = ""

            fh = open(f, 'rU')
            fileindex += 1
            j = fh.read()
            fh.close()

            try:
                j = json.loads(j, object_pairs_hook=OrderedDict)
                if not isinstance(j, type(OrderedDict())):
                    error_message = "File " + f + " did not contain a json object, i.e. {}."
                    error_list.append(error_message)
            except:
                error_message = "File " + f + " did not contain valid JSON."
                error_list.append(error_message)

            if not error_message:
                # add the object to the output file.
                try:
                    writer.writerow(j)
                    success_index += 1
                except:
                    error_message = "Error writing " + f + \
                        " to NDJSON. " + str(sys.exc_info())
                    error_list.append(error_message)

        if error_list:
            response_dict['num_files_attempted'] = fileindex
            response_dict['num_files_imported'] = success_index
            response_dict['num_file_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400
            response_dict['message'] = "Completed with errors."
        else:

            response_dict['num_files_attempted'] = fileindex
            response_dict['num_files_imported'] = success_index
            response_dict['num_file_errors'] = len(error_list)
            response_dict['code'] = 200
            response_dict['message'] = "Completed without errors."

    except:
        response_dict = {}
        response_dict['num_files_attempted'] = fileindex
        response_dict['num_files_imported'] = success_index
        response_dict['code'] = 500
        response_dict['errors'] = [str(sys.exc_info()), ]
        response_dict['message'] = str(sys.exc_info())

    return response_dict

if __name__ == "__main__":

    # Parse args
    parser = argparse.ArgumentParser(
        description='Load in directory which contains JSON docs and convert it into a single NDJSON file.')
    parser.add_argument(
        dest='input_JSON_dir',
        action='store',
        help='Input the JSON dir to load here')
    parser.add_argument(
        dest='output_NDJSON_file',
        action='store',
        help="Enter the output filename")
    args = parser.parse_args()

    result = jsondir2ndjson(args.input_JSON_dir, args.output_NDJSON_file)

    # output the JSON transaction summary
    print(json.dumps(result, indent=4))
