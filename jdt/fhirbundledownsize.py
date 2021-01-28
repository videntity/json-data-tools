#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Alan Viars

import argparse
import json
from collections import OrderedDict

def bundle_stub():
    od = OrderedDict()
    od["resourceType"] = "Bundle"
    od["type"] = "transaction"
    od["entry"] = []
    return od


def fhirbundledownsize(bundle_object, output_size):
    """FHIR Bundle to downsized Bundles with max entries of output_size"""

    response_list = []
    i = 0
    total = 0
    if "entry" not in bundle_object.keys():
        error = "Failed to find entry [] list. This file does not appear to be a FHIR Bundle."
        raise Exception(error)
    num_of_entries = len(bundle_object['entry'])
    for e in bundle_object['entry']:
        if i == 0:
            # Create a new Bundle
            bundle = bundle_stub()
            bundle['entry'].append(e)
        elif int(output_size) % i == 0:
            bundle = bundle_stub()
            bundle['entry'].append(e)
        else:
            # append a new entry
            bundle['entry'].append(e)

        if i == int(output_size):
            response_list.append(bundle)
            i = 0
        elif total == num_of_entries-1:
            response_list.append(bundle)
        i += 1
        total += 1

    return response_list


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Load in FHIR Bunlde and save multiple FHIR Bundles.')
    parser.add_argument(
        dest='bundle_file',
        action='store',
        help='Input the FHIR Bundle.')
    parser.add_argument(
        dest='output_size',
        action='store',
        default="500",
        help="Enter the integer of the maximum number of entries per bundle.")
    args = parser.parse_args()

    in_fh = open(args.bundle_file, 'r')
    bundle_object = json.loads(in_fh.read(), object_pairs_hook=OrderedDict)
    print("The number of entries in the source file is %s" % (len(bundle_object['entry'])))
    result_list = fhirbundledownsize(bundle_object, args.output_size)
    i = 1
    for r in result_list:
        output_file = "%s_%s" % (args.bundle_file, i)
        out_fh = open(output_file, 'w')
        json.dump(r, out_fh)
        out_fh.close()
        print("%s file created." % (output_file))
        i += 1

    # output the JSON transaction summary
    print("%s FHIR Bundles created." % len(result_list))
