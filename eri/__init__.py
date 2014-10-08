import csv
from bson import ObjectId
from pymongo import MongoClient
from slugify import slugify

mongo = MongoClient()
election_results_collection = mongo.elections.electionresults

# Clear collections before running importer
election_results_collection.remove({})


def import_data(csv_filepath, commune):
    ''' Imports deputy wealth declaration data.
    From a CSV file into a MongoDB collection.
    :param csv_filepath: the path of the CSV file.
    :param commune: the name of the commune.
    '''
    data_table = create_data_table(csv_filepath)
    docs = create_election_result_documents(data_table, commune)
    persist_in_mongo(docs)

    return len(docs)


def create_data_table(csv_filepath):
    data_table = []

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data_table.append(row)
    return data_table


def create_election_result_documents(data_table, name):
    docs = []

    previous_type_name = ''

    for column_index in range(1, len(data_table[0])):
        doc_dict = {
            "_id": str(ObjectId()),
            'commune': {
                'name': name,
                'slug': slugify(name)
            }
        }
        doc_dict['parties'] = []

        for row_index in range(0, len(data_table)):
            if row_index == 0:
                doc_dict['year'] = int(data_table[row_index][column_index])
            elif row_index == 1:

                type_name = data_table[row_index][column_index]
                type_slug = slugify(type_name)

                if type_slug not in ['assembly', 'national', 'mayoral']:
                    doc_dict['round'] = 2
                    type_name = previous_type_name
                    type_slug = slugify(type_name)

                else:
                    doc_dict['round'] = 1
                    previous_type_name = type_name

                doc_dict['type'] = {
                    'name': type_name,
                    'slug': type_slug
                }

            else:
                #doc_dict[data_table[row_index][0]] = int(data_table[row_index][column_index])
                doc_dict['parties'].append(
                    {
                        'party': data_table[row_index][0],
                        'votes': int(data_table[row_index][column_index])
                    }
                )

        print doc_dict
        print
        docs.append(doc_dict)

    return docs


#The definition below saves the JSON documents in MongoDB
def persist_in_mongo(docs):
    for doc in docs:
        election_results_collection.insert(doc)
