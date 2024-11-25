#!/usr/bin/env python3

import sqlite3
import csv
import hashlib
import urllib
from pathlib import Path
from urllib.parse import unquote
import lxml.etree as ET
import requests
from pyparsing import lineEnd

import FoxmlWorker as FW
from saxonche import *
import xmltodict


class ScholarUtilities:

    def __init__(self):
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.conn = sqlite3.connect('scholar.db')
        self.conn.row_factory = sqlite3.Row
        self.rels_map = {'isMemberOfCollection': 'collection_pid',
                         'isMemberOf': 'collection_pid',
                         'hasModel': 'content_model',
                         'isPageOf': 'page_of',
                         'isSequenceNumber': 'sequence',
                         'isConstituentOf': 'constituent_of'
                         }
        self.mods_xsl = "assets/rosies_transform.xsl"

    # Returns disk address from PID.
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

    # Processes CSV returned from direct objectStore harvest
    def process_clean_institution(self, institution, csv_file):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists {institution}(
            pid TEXT PRIMARY KEY,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constituent_of TEXT
            )""")
        self.conn.commit()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                collection = row['collection_pid']
                page_of = row['page_of']
                if not page_of:
                    page_of = ' '
                constituent_of = row['constituent_of']
                if not constituent_of:
                    constituent_of = ' '
                try:
                    command = f"INSERT OR REPLACE INTO  {institution} VALUES('{row['pid']}', '{row['content_model']}', '{collection}','{page_of}', '{row['sequence']}','{constituent_of}')"
                    cursor.execute(command)
                except sqlite3.Error:
                    print(command)
        self.conn.commit()

    # Adds all MODS records from datastreamStore to database
    def add_mods_to_database(self, namespace):
        cursor = self.conn.cursor()
        pids = self.get_pids_from_objectstore(namespace)
        for pid in pids:
            foxml_file = self.dereference(pid)
            foxml = f"{self.objectStore}/{foxml_file}"
            fw = FW.FWorker(foxml)
            if fw.get_state() != 'Active':
                continue
            mapping = fw.get_file_data()
            mods_info = mapping.get('MODS')
            if mods_info:
                mods_path = f"{self.datastreamStore}/{self.dereference(mods_info['filename'])}"
                mods_xml = Path(mods_path).read_text()
                if mods_xml:
                    mods_xml = mods_xml.replace("'", "''")
                    command = f"""UPDATE {namespace} set mods = '{mods_xml}' where pid = '{pid}'"""
                    cursor.execute(command)
        self.conn.commit()

    def extract_from_mods(self, pid):
        cursor = self.conn.cursor()
        command = f"SELECT MODS from IMAGINED where PID = '{pid}'"
        mods = cursor.execute(command).fetchone()['MODS']
        if not mods:
            return {}
        with PySaxonProcessor(license=False) as proc:
            xsltproc = proc.new_xslt30_processor()
            document = proc.parse_xml(xml_text=mods)
            executable = xsltproc.compile_stylesheet(stylesheet_file=self.mods_xsl)
            output = executable.transform_to_string(xdm_node=document)
        result = xmltodict.parse(output)['row']
        result['field_pid'] = pid
        return result

    def get_structure(self, table, output_file):
        cursor = self.conn.cursor()
        all_rows = []
        headers = []
        container_types = ['islandora:collectionCModel', 'islandora:bookCModel', 'islandora:compoundCModel']
        statement = f"select pid, collection_pid from {table} where content_model in {str(tuple(container_types))}"
        for row in cursor.execute(statement):
            all_rows.append(dict(row) | self.extract_from_mods(row['pid']))
            for row in all_rows:
                for key, value in row.items():
                    if key not in headers:
                        headers.append(key)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in all_rows:
                writer.writerow(row)

    # Take copied and pasted text from database query and transform to pid mapping.
    def text_to_csv(self,infile, outfile):
        with open(outfile, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['nid', 'pid'])
            writer.writeheader()
            with open(infile) as file:
                while line := file.readline():
                    line = line.replace(' ', '').replace('+', '').replace('-','').strip()
                    if not line or 'entity_id'  in line:
                        continue

                    row = {}
                    line_parts = line.split('|')
                    row['nid'] = line_parts[1]
                    row['pid'] = line_parts[2]
                    writer.writerow(row)


SU = ScholarUtilities()
SU.extract_from_mods('imagined:209201')
SU.text_to_csv('inputs/database_output.txt', 'outputs/transformed.csv')
