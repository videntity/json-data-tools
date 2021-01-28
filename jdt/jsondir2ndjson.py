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


def jsonlist2ndjson(json_object_list):
    return ndjson.dumps(json_object_list)


def get_json_list_from_directory(json_dir):
    json_object_list = []
    onlyfiles = []
    for root, dirs, files in os.walk(json_dir):
        for file in files:
            if file.endswith(".json") or file.endswith(".js"):
                onlyfiles.append(os.path.join(root, file))

    for f in onlyfiles:
        j = None
        error_message = ""
        fh = open(f, 'rU')
        j = fh.read()
        fh.close()

        try:
            j = json.loads(j, object_pairs_hook=OrderedDict)
            if not isinstance(j, type(OrderedDict())):
                error_message = "File " + f + " did not contain a json object, i.e. {}."
                print(error_message)
                sys.exit(1)
        except Exception as e:
            error_message = "File " + f + " did not contain valid JSON."
            print(error_message, e)
            sys.exit(1)

        json_object_list.append(j)
    return json_object_list


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
            except Exception:
                error_message = "File " + f + " did not contain valid JSON."
                error_list.append(error_message)

            if not error_message:
                # add the object to the output file.
                try:
                    writer.writerow(j)
                    success_index += 1
                except Exception:
                    error_message = "Error writing " + f + " to NDJSON. " + str(sys.exc_info())
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

    except Exception:
        response_dict = {}
        response_dict['num_files_attempted'] = fileindex
        response_dict['num_files_imported'] = success_index
        response_dict['code'] = 500
        response_dict['errors'] = [str(sys.exc_info()), ]
        response_dict['message'] = str(sys.exc_info())

    return response_dict


if __name__ == "__main__":

    # Parse CLI args
    parser = argparse.ArgumentParser(
        description="""Load in directory, which contains JSON documents,
with .json or .js extensions, and convert it into a single NDJSON file.""")

    parser.add_argument(
        dest='input_json_dir',
        action='store',
        help='Input the JSON dir to load here')
    parser.add_argument(
        dest='output_ndjson_file',
        action='store',
        help="Enter the output filename")

    # load in cli parser ags
    args = parser.parse_args()

    # get a list of all JSON objects from a local file path.
    #
    # It gets all .json and .js files recursivly.
    object_list = get_json_list_from_directory(args.input_json_dir)

    # Open the output file
    out_fh = open(args.output_ndjson_file, 'w')

    # output the ndjson file.
    output = jsonlist2ndjson(object_list)
    out_fh.write(output)

    # close the file handle
    out_fh.close()
