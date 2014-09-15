import argparse
import os
from eri import import_data

parser = argparse.ArgumentParser()
parser.add_argument(
    '--csvDirectoryPath',
    type=str, default='data',
    help='The CSV directory path.')

args = parser.parse_args()
csv_directory_path = args.csvDirectoryPath

# Run the app
if __name__ == '__main__':
    result_dict = {}

    if os.path.isdir(csv_directory_path):
        for filename in os.listdir(csv_directory_path):
            if(filename.endswith(".csv")):

                csv_filepath = csv_directory_path + '/' + filename
                commune = filename.replace('.csv', '')

                print "\n\nImporting %s election results from '%s':\n" % (commune, csv_filepath)
                doc_count = import_data(csv_filepath, commune)

                result_dict[commune] = doc_count

    print "\n\nIMPORT SUMMARY:"
    for key in result_dict.keys():
        print "%s election results imported for %s." % (result_dict[key], key)

    print  # Just skip line
