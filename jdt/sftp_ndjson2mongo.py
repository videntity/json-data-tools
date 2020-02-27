#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
# Transparent Health

import os
import pysftp
from jdt.csv2mongo import csv2mongo
from jdt.ndjson2mongo import ndjson2mongo


def download_files_from_ftp(
        sftp_host,
        sftp_username,
        sftp_private_key_path,
        known_hosts_path,
        remote_path_to_download,
        local_destination_path=""):
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(known_hosts_path)
    # print("Connect to", myHostname, myUsername, private_key,)
    localpaths = []
    with pysftp.Connection(sftp_host, username=sftp_username,
                           private_key=sftp_private_key_path, cnopts=cnopts) as sftp:

        print("SFTP Connection successfully established...")
        response_dict = {}
        sftp.cwd(remote_path_to_download)

        # Switch to a remote directory

        # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir_attr()

        # Print data
        for attr in directory_structure:
            #print(attr.filename, attr)
            # Define the file that you want to download from the remote
            # directory
            remoteFilePath = attr.filename
            localFilePath = attr.filename
            if local_destination_path:
                localFilePath = os.path.join(
                    local_destination_path, localFilePath)
            sftp.get(remoteFilePath, localFilePath)
            localpaths.append(localFilePath)

    return localpaths


def import_files_into_mongo(list_of_downloaded_filepaths, database_name,
                            delete_collection_before_import, host, port):
    for path in list_of_downloaded_filepaths:
        splitpath = os.path.split(path)
        colection_name, extension = splitpath[-1].rsplit(".", 1)
        if path.endswith("ndjson"):
            colection_name = "%s-fhir" % (colection_name)
            print(ndjson2mongo(path, database_name, colection_name,
                               delete_collection_before_import, host, port))

        if path.endswith("csv"):
            colection_name = "%s-csv" % (colection_name)
            print(csv2mongo(path, database_name, colection_name,
                            delete_collection_before_import, host, port))
