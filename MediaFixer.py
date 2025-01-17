

def make_media_delete_sheet(filename):
    with open(filename, 'r') as file:
        # Iterate over each line in the file
        with open('outputs/delete_node_media.csv', 'w' ) as outfile:
            outfile.write('node_id\n')
            for line in file:
                identifier = line.split('_')[1]
                pid = f'imagined:{identifier}'
                outfile.write(line.split('_')[1] + '\n')



make_media_delete_sheet('inputs/file_list.txt')