#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4

from collections import OrderedDict
import ndjson
import requests


def ndjson2fhir(ndjsonfile, fhir_base_url):
    """Return a response_dict with a summary of ndjson2fhir transaction."""
    # print("Start the import of", ndjsonfile, "into", fhir_base_url)
    response_dict = OrderedDict()
    fileindex = 0
    index = 0
    error_list = []
    ids = []
    with open(ndjsonfile) as f:
        data = ndjson.load(f, object_pairs_hook=OrderedDict)
        for item in data:
            try:
                if not isinstance(item, type(OrderedDict())):
                    error_message = "File " + \
                        str(item) + " did not contain a JSON object, i.e. {}."
                    error_list.append(error_message)
                # insert the item/document using an HTTP POST to a FHIR server
                resource_url = "%s%s" % (fhir_base_url, item["resourceType"])

                r = requests.post(resource_url, json=item, headers={
                                  'Content-type': 'application/json'})
                if r.status_code not in (201,):
                    error_message = "Item with ID " + \
                        item['id'] + " failed to POST/CREATE in the FHIR server."
                    error_list.append(error_message)
                else:
                    ids.append({item['id']: r.json()['id']})
                    index += 1
            except:
                # print(sys.exc_info())
                error_message = "File " + \
                    str(item) + " did not contain valid JSON."
                error_list.append(error_message)

        if error_list:
            response_dict['file'] = ndjsonfile
            response_dict['fhir_base_url'] = fhir_base_url
            response_dict['num_fhir_resources_created'] = index
            response_dict['ids'] = ids
            response_dict['num_upload_errors'] = len(error_list)
            response_dict['errors'] = error_list
            response_dict['code'] = 400
            response_dict['message'] = "Completed with errors."

        else:
            response_dict['file'] = ndjsonfile
            response_dict['fhir_base_url'] = fhir_base_url
            response_dict['num_fhir_resources_created'] = index
            response_dict['ids'] = ids
            response_dict['num_file_errors'] = len(error_list)
            response_dict['code'] = 200
            response_dict['message'] = "Completed without errors."

    return response_dict
