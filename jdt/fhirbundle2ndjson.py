#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Alan Viars

import argparse
import json
import sys
from collections import OrderedDict
import ndjson


def fhirbundle2ndjson(bundle_file, output_file):
    
    """FHIR Bundle to separate resources."""
    # print("Opening FHIR Bundle file:", bundle_file,)
  
    response_dict = OrderedDict()
    i = 0
    error_list = []
    
    out_fh = open(output_file, 'w')
    writer = ndjson.writer(out_fh)
    in_fh = open(bundle_file, 'r')
        
    response_dict['input_file'] = bundle_file
    response_dict['num_entries_converted'] = i
    response_dict['num_errors'] = 0

    item = json.loads(in_fh.read(), object_pairs_hook=OrderedDict)
    # print("JSON loaded successfully...")

    if "entry" not in item.keys():
            error = "Failed to find entry [] list."
            # print(error)
            error_list.append(error)
            response_dict['num_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400            
            response_dict['message'] = "Completed with errors."
            return response_dict
    i=0
    for e in item['entry']:
        try:
            writer.writerow(e['resource'])
            i+=1
        except Exception as e:
            error_message = "Error writing " + e + \
                    " to NDJSON. " + str(sys.exc_info(), + str(e))
            error_list.append(error_message)
            
    response_dict['input_file'] = bundle_file
    response_dict['output_file'] = output_file
    response_dict['errors'] = error_list
    response_dict['num_entries_imported'] = i
    response_dict['num_errors'] = len(error_list)
        
    if response_dict['num_errors']:
        response_dict['code'] = 400
        response_dict['message'] = "Completed with errors."
    else:
        response_dict['code'] = 200
        response_dict['message'] = "OK"

    return response_dict


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Load in FHIR Bunlde and save Resource entries to an NDJSON file.')
    parser.add_argument(
        dest='bundle_file',
        action='store',
        help='Input the FHIR Bundle.')
    parser.add_argument(
        dest='output_file',
        action='store',
        help="Enter the output NDJSON filename")
    args = parser.parse_args()

    result = fhirbundle2ndjson(args.bundle_file, args.output_file)
    # output the JSON transaction summary
    print(json.dumps(result, indent=4))
