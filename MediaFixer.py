import ScholarUtilities

def make_media_delete_sheet(filename):

    with open(filename, 'r') as file:
        # Iterate over each line in the file
        with open('outputs/nodes_to_archive.csv', 'w' ) as outfile:
            outfile.write('(')
            for line in file:
                identifier = line.split('_')[1]
                pid = f'imagined:{identifier}'
                outfile.write(f"'{pid}',\n")
            outfile.write(')')

def change_file_name(filename):
    su = ScholarUtilities.ScholarUtilities()
    with open(filename, 'r') as file:
        with open('outputs/name_change.sh', 'w') as outfile:
            for line in file:
                parts = line.split('_')
                nid = su.get_nid_from_pid('imagined', parts[0])
                outfile.write(f"mv {line.strip()} {nid}_OBJ.ppm\n")

def make_ppm_ingest():
    with open('outputs/name_change.sh') as input:
        with open('outputs/ppm_add_media.csv', 'w') as outfile:
            outfile.write("node_id,file\n")
            for line in input:
                filename = line.strip().split('.ppm ')[1]
                nid = filename.split('_')[0]
                outfile.write(f"{nid},{filename}\n")

def build_signature_sheet():
    su = ScholarUtilities.ScholarUtilities()
    with open('inputs/signature_filelist.txt') as input:
        with open('outputs/add_signature_media', 'w') as outfile:
            outfile.write('node_id,file\n')
            for line in input:
                pid = line.split('_sig')[0].replace('_', ':')
                nid = su.get_nid_from_pid('islandscholar', pid)
                outfile.write(f"{nid},{line}")

build_signature_sheet()






