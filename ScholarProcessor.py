#!/usr/bin/env python3

import csv
import re
import shutil
import time
from os import write
from pathlib import Path
import sqlite3
import lxml.etree as ET
import FoxmlWorker as FW
import ScholarUtilities as SU


class ScholarProcessor:

    def __init__(self):
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.conn = sqlite3.connect('scholar.db')
        self.conn.row_factory = sqlite3.Row
        self.su = SU.ScholarUtilities()
        self.scholar = 'https://scholar.researchspaces.ca'
        self.content_model_primary_map = {
            'ir:citationCModel': '',
            'ir:thesisCModel': 'PDF',
            'islandora:sp_pdf': 'PDF'
        }
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "image/tif": ".tif",
                        "text/xml": ".xml",
                        "text/plain": ".txt",
                        "application/pdf": ".pdf",
                        "application/xml": ".xml",
                        "audio/x-wav": ".wav"
                        }

    # Takes csv generated on productions server and updates the database with each object with its hierarchy.
    def populate_database(self, csv_file):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists islandscholar(
            pid TEXT PRIMARY KEY,
            nid TEXT,
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
                nid = row['nid']
                if not nid:
                    nid = ''
                page_of = row['page_of']
                if not page_of:
                    page_of = ' '
                constituent_of = row['constituent_of']
                if not constituent_of:
                    constituent_of = ' '
                try:
                    command = f"INSERT OR REPLACE INTO islandscholar VALUES('{row['pid']}', '{nid}' '{row['content_model']}', '{collection}','{page_of}', '{row['sequence']}','{constituent_of}')"
                    cursor.execute(command)
                except sqlite3.Error:
                    print(row['PID'])
        self.conn.commit()

    # Updates the database to include nids from the new system mapped to exising pids.
    def update_pid_nid_mapping(self, csv_file):
        cursor = self.conn.cursor()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                statement = f"update islandscholar set nid = '{row['node_id']}' where pid = '{row['field_pid']}'"
                cursor.execute(statement)
        self.conn.commit()

    # Builds workbench sheet to ingest primary assets from current site using RESTFULL interface
    def build_workbench_sheet(self, collection_pid):
        output_file_name = f"{collection_pid.replace(':', '_')}_workbench.csv"
        cursor = self.conn.cursor()
        statement = f"""
        SELECT nid, pid, content_model
        FROM islandscholar
        WHERE collection_pid = '{collection_pid}'
        and nid IS NOT NULL
"""
        headers = ['node_id', 'file']
        with open(output_file_name, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in cursor.execute(statement):
                datastream = self.content_model_primary_map[row['content_model']]
                if not datastream:
                    continue
                filename = f"{self.scholar}/islandora/object/{row['pid']}/datastreams/{datastream}/download"
                writer.writerow({'node_id': row['nid'], 'file': filename})

    # Builds workbench sheet to ingest primary assets harvesting from the Fedora data dir.
    def build_workbench_sheet_remote(self):
        output_file_name = f"imagined_add_media_workbench.csv"
        cursor = self.conn.cursor()
        statement = f"""
                SELECT nid, pid, content_model
                FROM imagined
                WHERE nid IS NOT NULL
        """
        headers = ['node_id', 'file']
        filepath = 'workbench_files'
        sheetpath = 'workbench_sheets'
        # Build directory
        Path(filepath).mkdir(parents=True, exist_ok=True)
        Path(sheetpath).mkdir(parents=True, exist_ok=True)
        with open(f"{sheetpath}/{output_file_name}", 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in cursor.execute(statement):
                foxml_file = self.su.dereference(row['pid'])
                foxml = f"{self.objectStore}/{foxml_file}"
                fw = FW.FWorker(foxml)
                if fw.properties['state'] != 'Active':
                    continue
                all_datastreams = fw.get_file_data()
                original_file = False
                if 'PDF' in all_datastreams:
                    original_file = 'PDF'
                if 'OBJ' in all_datastreams:
                    original_file = 'OBJ'
                if original_file in all_datastreams:
                    datastream_data = all_datastreams[original_file]
                    source = f"{self.datastreamStore}/{self.su.dereference(datastream_data['filename'])}"
                    mime_ext = '.bin'
                    if datastream_data['mimetype'] in self.mimemap:
                        mime_ext = self.mimemap[datastream_data['mimetype']]
                    destination = f"{row['nid']}_{original_file}{mime_ext}"
                    shutil.copy(source, f"{filepath}/{destination}")
                    writer.writerow({'node_id': row['nid'], 'file': destination})
        self.conn.close()

    def build_workbench_mods_sheet_remote(self):
        output_file_name = f"imagined_add_mods_workbench.csv"
        cursor = self.conn.cursor()
        statement = f"""
                   SELECT nid, mods
                   FROM imagined
                   WHERE nid IS NOT NULL
           """
        headers = ['node_id', 'file']
        filepath = 'workbench_files'
        sheetpath = 'workbench_sheets'
        # Build directory
        Path(filepath).mkdir(parents=True, exist_ok=True)
        Path(sheetpath).mkdir(parents=True, exist_ok=True)
        with open(f"{sheetpath}/{output_file_name}", 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in cursor.execute(statement):
                if row['mods']:
                    filename = f"{row['nid'].replace(':', '_')}_mods.xml"
                    with open(f"{filepath}/{filename}", "w") as text_file:
                        text_file.write(row['mods'])
                    writer.writerow({'node_id': row['nid'], 'file': filename})
        self.conn.close()

    def build(self, csv_file):
        cursor = self.conn.cursor()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    # Using parameterized queries to prevent SQL injection
                    command = "INSERT OR REPLACE INTO missing_mods (pid, nid) VALUES (?, ?)"
                    cursor.execute(command, (row['pid'], row['nid']))
                except sqlite3.Error as e:
                    print(f"SQLite error: {e}, row: {row}")
        self.conn.commit()

    def make_media_delete_sheet(self, filename):
        with open(filename, 'r') as file:
            # Iterate over each line in the file
            with open('outputs/delete_node_media.csv', 'w') as outfile:
                outfile.write('node_id\n')
                for line in file:
                    identifier = line.split('_')[1]
                    pid = f'imagined:{identifier}'
                    nid = self.su.get_nid_from_pid('imagined', pid)
                    outfile.write(nid + '\n')

    def prepare_page_worksheet(self, output_file):
        details = self.mu.get_page_details('msvu')
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for detail in details:
                mods = self.mu.extract_from_mods(detail['field_pid'])
                row = mods | detail
                node_id = self.mu.get_nid_from_pid('msvu', row['field_member_of'])
                row['id'] = row['field_pid']
                if node_id:
                    row['field_member_of'] = node_id
                    writer.writerow(row)


if __name__ == '__main__':
    SP = ScholarProcessor()
    SP.make_media_delete_sheet('inputs/imagined_ppm_files.txt')
