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


def jsonlist2ndjson(jsonlistinput_filename, output_filename):
    """Convert a single json list of json objects to a single ndjson file."""


    error_list = []
    success_index = 0
    response_dict = OrderedDict()

    try:
        # get the files in the specified directory
        print("Getting a list from from file", jsonlistinput_filename)
        in_fh = open(jsonlistinput_filename, 'r')

        # Writing the
        out_fh = open(output_filename, 'w')
        writer = ndjson.writer(out_fh)
        print("Outputting file", jsonlistinput_filename)
        reader = in_fh.read()
        thelist = json.loads(reader, object_pairs_hook=OrderedDict)

        for i in thelist:

            if not isinstance(i, type(OrderedDict())):
                error_message = "File " + f + " did not contain a json object, i.e. {}."
                print(error_message)
            else:
                writer.writerow(i)
                success_index+=1
        in_fh.close()
        out_fh.close()

    except Exception as e:
            print("error", e)
            error_message = e
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
    
    response_dict = {"error_list": error_list, 
                     "input_file":  jsonlistinput_filename, 
                      "output_file":  output_filename,
                      "number_processed": success_index,
                      "number_in_input": len(thelist) 
                     }
    return response_dict


if __name__ == "__main__":
    
    # Parse args
    parser = argparse.ArgumentParser(
        description='Load in file with a JSON list that contains a JSON objects and convert it into a single NDJSON file.')

    parser.add_argument(
        dest='input_JSON',
        action='store',
        help='Input the JSON list to load here')

    parser.add_argument(
        dest='output_NDJSON_file',
        action='store',
        help="Enter the output NDJSON filename")

    args = parser.parse_args()

    result = jsonlist2ndjson(args.input_JSON, args.output_NDJSON_file)

    # output the JSON transaction summary
    print(json.dumps(result, indent=4))
