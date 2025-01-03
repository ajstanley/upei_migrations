import csv


def clean_ri(input_file):
    lines = []
    with open(input_file) as file:
        ri_lines = [line.rstrip() for line in file]
    for ri_line in ri_lines:
        parts = ri_line.split(' ')
        lines.append(parts[0].replace('<info:fedora/', '').replace('>', ''))

    with open('cleaned_ir.txt', 'w') as f:
        for line in lines:

            f.write(f"{line}\n")


def compare_file(file1, file2):
    with open(file1) as file:
        first = [line.rstrip() for line in file]
    with open(file2) as file:
        second = [line.rstrip() for line in file]
    diff = list(set(second) - set(first))
    for entry in diff:
        print (f"'{entry}',")


compare_file('inputs/scholar_pid.csv','cleaned_ir.txt')