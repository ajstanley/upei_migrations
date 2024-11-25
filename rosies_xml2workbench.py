#!/Users/rlefaive/.venv/bin/python
import csv
import html
import sys
import xml.etree.ElementTree as ET
import os
from glob import glob
import random
import re
import edtf_validate.valid_edtf
from edtf import text_to_edtf

def process_files(filenames,
                  output_filename,
                  fieldnames,
                  formatted_text_fieldnames = [],
                  single_valued_fieldnames = [],
                  collections_map = {},
                  id_column = 'field_pid',
                  link_fieldnames = [],
                  edtf_fieldnames = [],
                  fieldname_rewrites = {},
                  created_dates_map = {},
                  departments_map = {},
                  scholars_map = {}):
    files_processed = 0
    files_written = 0
    fieldnames.append('id')
    fieldnames.remove('field_model') # Fixme add this back in when we have a model.
    with open(output_filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for filename in filenames:
            files_processed += 1
            # FIXME Filter out a sample
            #if random.random() > 0.0625:
            #    continue
            tree = ET.parse(filename)
            row = tree.getroot()

            # Prepare data for workbench
            data = {}
            for leaf in row:

                if leaf.text is None or leaf.text.strip() == '' or leaf.text == 'T00:00:00Z':
                    continue
                if leaf.tag not in fieldnames:
                    if leaf.tag in fieldname_rewrites.keys():
                        leaf.tag = fieldname_rewrites[leaf.tag]
                    else:
                        print("Field name not found in fieldnames: [{}]".format(leaf.tag,))
                        continue

                if '|' in leaf.text:
                    solved = process_pipe_exceptions(leaf)
                    if not solved:
                        print("ERROR: Text value contains illegal pipe character: [{}]".format(leaf.text))

                # Un-encode XML ampersands.
                if '&amp;' in leaf.text:
                    leaf.text = leaf.text.replace('&amp;', '&')

                if leaf.tag in data.keys():
                    data[leaf.tag].append(leaf.text)
                else:
                    data[leaf.tag] = [leaf.text]

                if leaf.tag == id_column:
                    data['id'] = [leaf.text]

            # Combine formatted text-type field instances (abstract) into one field.
            for field in formatted_text_fieldnames:
                if field in data.keys():
                    combined_value = ''
                    for instance in data[field]:
                        if not(instance.startswith('<p>')):
                            element = ET.Element('p')
                            element.text = instance
                            combined_value += ET.tostring(element, encoding='unicode')
                        else:
                            combined_value += instance
                    combined_value = html.unescape(combined_value)
                    combined_value = html.unescape(combined_value)
                    data[field] = [combined_value]

            # Validate single-valued fields
            for field in single_valued_fieldnames:
                if field in data.keys():
                    if len(data[field]) > 1:
                        print("ERROR: single valued field [{}] has multiple values.".format(field))

            # Map collections to field_member_of.
            if 'field_member_of' in data.keys():
                collection_ids = []
                for instance in data['field_member_of']:
                    if instance in collections_map.keys():
                        collection_ids.append(collections_map[instance])
                    else:
                        print("ERROR: Collection not found in collection map: [{}]".format(instance))
                data['field_member_of'] = collection_ids

            # Map created_dates from fedora.
            date_created = created_dates_map[data['field_pid'][0]].replace('Z', '+00:00')
            data['created'] = [date_created]

            # Put in genres when missing.
            if 'field_genre' not in data.keys() or data['field_genre'] == '':
                data['field_genre'] = ['unknown']
                # FIXME this is a hack, put an actual value here.

            # put in csl genres where missing.
            if 'field_csl_type' not in data.keys() or data['field_csl_type'] == '':
                data['field_csl_type'] = ['document']
                # FIXME this is also a hack; put an actual value here.

            # Fix Masters to Master.
            if 'field_etd_degree_level' in data.keys():
                if data['field_etd_degree_level'] == ['Masters']:
                    data['field_etd_degree_level'] = ['Master']

            if 'field_note' in data.keys():
                new_notes = []
                for value in data['field_note']:
                    if value.startswith("Source type"):
                        continue
                    if value == ':':
                        continue
                    if value == 'Statement of responsibility:':
                        continue
                    else:
                        new_notes.append(value)
                data['field_note'] = new_notes

            # Deal with link fields that need http things.
            for field in link_fieldnames:
                links = []
                if field in data.keys():
                    for instance in data[field]:
                        if instance.startswith('http'):
                            links.append(instance)
                        else:
                            new_value = process_link_exception(instance, data)
                            if new_value is not None:
                                links.append(new_value)
                    data[field] = links

            # Validate EDTF values.
            for field in edtf_fieldnames:
                dates = set()
                if field in data.keys():
                    for instance in data[field]:
                        # Ignore blanks.
                        if instance == 'T00:00:00Z':
                            continue

                        # validate EDTF
                        ok = validate_edtf_date(instance)
                        if ok:
                            dates.add(instance)
                            continue

                        # process exceptional values manually.
                        sub = process_date_exceptions(data[id_column][0], field, instance)
                        if sub is not None:
                            dates.add(sub)
                            continue

                        # Parse dates to EDTF using text_to_edtf
                        # (a bit iffy, which is why we take care of exceptions above.)
                        parsed = text_to_edtf(instance)
                        if parsed is None:

                            # Dates that fail parsing
                            if instance == '2021-04-31':
                                instance = '2021-04-30'
                                dates.add(instance)
                            elif instance == '2022-11-31':
                                instance = '2022-11-30'
                                dates.add(instance)
                            else:
                                print("Field: [{}] BAD DATE: [{}] in file [{}]".format(field, instance, filename))
                        else:
                            # These are all good dates, Brent.
                            if field not in ['field_date_issued', 'field_date_submitted', 'field_host_date_issued',
                                             'field_host_date_copyrighted']:
                                print('Field: [{}] good date: [{}] from [{}] in file [{}]'.format(field, parsed, instance, filename))
                            dates.add(parsed)

                    data[field] = list(dates)

            # Dedupe other fields not in EDTF:
            if field in ['field_part_date']:
                if len(data[field]) > 1:
                    data[field] = list(set(data[field]))

            # Map departments to term_ids
            if 'field_department' in data.keys():
                values = []
                for value in data['field_department']:
                    key = value.strip().replace('<br/>','').lower()

                    if key not in departments_map.keys():
                        print("Department not in lookup: [{}]".format(key))
                        continue
                    else:
                        values.append(departments_map[key])
                data['field_department'] = values

            # Map scholars to their IDs.
            if 'field_scholar' in data.keys():
                values = []
                for value in data['field_scholar']:
                    key = value.strip().replace('<br/>','').replace('@upei.ca','').lower()

                    # Corrections - this should be done in preprocess!
                    if key in ['correction--2005']:
                        continue
                    elif key == 'wmwhelan':
                        key='wwhelan'
                    elif key == 'pl':
                        key='plmckenna'

                    elif key == '9606':
                        continue

                    if key not in scholars_map.keys():
                        print("Scholar not in lookup: [{}]".format(key))
                        values.append(key)

                    else:
                        values.append(scholars_map[key])
                data['field_scholar'] = values

            # Merge with pipe characters
            for field, values in data.items():
                data[field] = '|'.join(values)

            # Write to output.
            #print(data)
            writer.writerow(data)
            files_written += 1

    print("Files processed: [{}]".format(files_processed))
    print("Files written: [{}]".format(files_written))


def process_pipe_exceptions(leaf):
    if leaf.text == 'Healthcare Policy | Politiques de Santé':
        leaf.text = leaf.text.replace('|', '=')
        return True
    if leaf.text.startswith('In [M.R. Burke, Large entire'):
        with open('ir_9780-abstract.txt', 'w') as f:
            f.write(leaf.text)
        leaf.text = 'FIXME'
        return True
    if leaf.text.startswith('1. 1.|Decreasing temperature stimuli produced responses and threshold similar'):
        with open('ir_1065-abstract.txt', 'w') as f:
            f.write(leaf.text)
        leaf.text = 'FIXME'
        return True
    if leaf.text.startswith('Monte Carlo simulations are used to study the behavior of two polymers under confinement'):
        with open('ir_10271-abstract.txt', 'w') as f:
            f.write(leaf.text)
        leaf.text = 'FIXME'
        return True
    if leaf.text == 'La Revue Riviere | The River Review':
        leaf.text = leaf.text.replace('|', '=')
        return True
    if leaf.text == 'Related blog posting at Network in Canadian History & Environment | Nouvelle initiative Canadienne en histoire de l\'environnement. Available at http://islandscholar.ca/islandora/object/ir:21203.':
        leaf.text = leaf.text.replace('|', '=')
        return True
    if leaf.text == 'xiv, 149 leaves, bound :|bill. ;|c29 cm. Bibliography: leaves 87-91.':
        leaf.text = 'xiv, 149 leaves, bound : ill. ; 29 cm. Bibliography: leaves 87-91.'
        return True
    return False

def process_date_exceptions(pid, field, value):
    lookups = {
        'ir:20510': {'field_host_date_issued': {'216': '2016'}},
        'ir:22021': {'field_host_date_issued': {'2': '2018'}},
        'ir:24501': {'field_host_date_issued': {'286': '2020-04'}},
        'ir:7435': {'field_host_date_copyrighted': {'206': '2006'}},
    }
    if pid in lookups.keys():
        if field in lookups[pid].keys():
            if value in lookups[pid][field].keys():
                return lookups[pid][field][value]
    return None

def process_link_exception(value, data):
    if value.startswith('www.'):
        return 'http://' + value
    if value.startswith('10.'):
        return 'https://doi.org/' + value
    if value.startswith('This paper develops an equilibrium'):
        return None
    if value in ['Charlottetown, Prince Edward Island', 'Knoxville, TN, USA', 'Albuquerque, NM, USA', 'Knoxville, TN, USA', 'San Juan, Puerto Rico, USA', 'San Juan, Puerto Rico, USA', 'Toronto, ON, Canada', 'Knoxville, TN, USA', 'New Orleans, LA, USA (Hybrid Conference)', 'Saskatoon']:
        return None
    if value == '\\':
        return None

    print('pid: [{}], value: [{}]'.format(data['id'], value))
    return None

def validate_edtf_date(date):
    date = date.strip()
    # nnnX?
    if re.match(r"^[1-2]\d\dX\?", date):
        return True
    # nnXX?
    elif re.match(r"^[1-2]\dXX\?", date):
        return True
    # nXXX?
    elif re.match(r"^[1-2]XXX\?", date):
        return True
    # nXXX~
    elif re.match(r"^[1-2]XXX\~", date):
        return True
    # nnXX~
    elif re.match(r"^[1-2]\dXX\~", date):
        return True
    # nnnX~
    elif re.match(r"^[1-2]\d\dX\~", date):
        return True
    # nXXX%
    elif re.match(r"^[1-2]XXX\%", date):
        return True
    # nnXX%
    elif re.match(r"^[1-2]\dXX\%", date):
        return True
    # nnnX%
    elif re.match(r"^[1-2]\d\dX\%", date):
        return True
    # XXXX?
    elif re.match(r"^XXXX\?", date):
        return True
    # XXXX~
    elif re.match(r"^XXXX\~", date):
        return True
    # XXXX%
    elif re.match(r"^XXXX\%", date):
        return True
    elif edtf_validate.valid_edtf.is_valid(date):
        return True
    else:
        return False

def main():
    directory_name = sys.argv[1]
    filenames = glob(os.path.join(directory_name, '*.xml'))
    if len(filenames) == 0:
        print("no files found.")
        exit()

    output_filename = sys.argv[2]

    # Import fieldnames from file, store in list.
    fieldnames = []
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path,'xml2workbench-fieldnames.txt'), 'r') as f:
        for line in f:
            fieldnames.append(line.strip())

    fieldname_rewrites = {'field_copyright_date': 'field_date_copyrighted',
                          'field_edtf_date_issued': 'field_date_issued',
                          'field_edtf_date_created': 'field_date_submitted',
                          'field_host_date': 'field_host_date_other',
                          'field_host_copyright_date': 'field_host_date_copyrighted',
                          }

    formatted_text_fieldnames = ['field_abstract']
    single_valued_fieldnames = ['title', 'field_full_title', 'field_description', 'field_abstract',
                                'field_table_of_contents', 'field_resource_type', 'field_model', 'field_weight',
                                'field_viewer_override', 'field_edtf_date_issued', 'field_host_date_other']
    link_fieldnames = ['field_location_url']
    edtf_fieldnames = ['field_date_copyrighted', 'field_date_issued', 'field_date_submitted', 'field_host_date_issued',
                       'field_host_date_copyrighted']

    collections_map = {}
    with open('collections_mapping.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            collections_map[row[0]] = row[1]

    created_dates_map = {}
    with open('created_dates_mapping.txt', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            created_dates_map[row[0]] = row[1]

    departments_map = {}
    with open('../../1-MADS/0-departments/3a-departments.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['u2'] is not None:
                departments_map[row['u2'].lower()] = row['term_id']

    scholars_map = {}
    with open('../../1-MADS/1-scholars/3a-scholars.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['u1'] is not None:
                scholars_map[row['u1'].lower()] = row['term_id']
    # Prepare output file
    process_files(filenames, output_filename, fieldnames, formatted_text_fieldnames, single_valued_fieldnames,
                  collections_map, 'field_pid', link_fieldnames, edtf_fieldnames, fieldname_rewrites,
                  created_dates_map, departments_map, scholars_map)

if __name__ == '__main__':
    main()