#!/usr/bin/env python3

import sqlite3
import csv
import hashlib
import urllib
from pathlib import Path
from urllib.parse import unquote
import lxml.etree as ET
import requests
import FoxmlWorker as FW
class ScholarUtilities:

    def __init__(self):
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.conn = sqlite3.connect('scholar.db')
        self.conn.row_factory = sqlite3.Row


    def dereference(self, identifier: str) -> str:
        # Replace '+' with '/' in the identifier
        slashed = identifier.replace('+', '/')
        full = f"info:fedora/{slashed}"

        # Generate the MD5 hash of the full string
        hash_value = hashlib.md5(full.encode('utf-8')).hexdigest()

        # Pattern to fill with hash (similar to the `##` placeholder)
        subbed = "##"

        # Replace the '#' characters in `subbed` with the corresponding characters from `hash_value`
        hash_offset = 0
        pattern_offset = 0
        result = list(subbed)

        while pattern_offset < len(result) and hash_offset < len(hash_value):
            if result[pattern_offset] == '#':
                result[pattern_offset] = hash_value[hash_offset]
                hash_offset += 1
            pattern_offset += 1

        subbed = ''.join(result)
        # URL encode the full string, replacing '_' with '%5F'
        encoded = urllib.parse.quote(full, safe='').replace('_', '%5F')
        return f"{subbed}/{encoded}"
    # Gets PIDS, filtered by namespace directly from objectStore
    def get_pids_from_objectstore(self, namespace=''):
        wildcard = '*/*'
        if namespace:
            wildcard = f'*/*{namespace}*'
        pids = []
        for p in Path(self.objectStore).rglob(wildcard):
            pid = unquote(p.name).replace('info:fedora/', '')
            pids.append(pid)
        return pids
    # Gets RELS-EXT relationships from objectStore
    def build_record_from_pids(self, namespace, output_file):
        pids = self.get_pids_from_objectstore(namespace)
        headers = ['pid',
                   'content_model',
                   'collection_pid',
                   'page_of',
                   'sequence',
                   'constituent_of']

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for pid in pids:
                foxml_file = self.dereference(pid)
                foxml = f"{self.objectStore}/{foxml_file}"
                fw = FW.FWorker(foxml)
                if fw.get_state() != 'Active':
                    continue
                relations = fw.get_rels_ext_values()
                row = {}
                row['pid'] = pid
                for relation, value in relations.items():
                    if relation in self.rels_map:
                        row[self.rels_map[relation]] = value
                writer.writerow(row)