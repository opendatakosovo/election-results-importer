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
    :param commune: the slug of the commune.
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


def create_election_result_documents(data_table, commune):
    docs = []

    for column_index in range(1, len(data_table[0])):
        doc_dict = {
            "_id": str(ObjectId()),
            'commune': commune
        }

        for row_index in range(0, len(data_table)):
            if row_index == 0:
                doc_dict['year'] = int(data_table[row_index][column_index])
            elif row_index == 1:
                doc_dict['type'] = slugify(data_table[row_index][column_index])
            else:
                doc_dict[data_table[row_index][0]] = int(data_table[row_index][column_index])

        print doc_dict
        print
        docs.append(doc_dict)

    return docs


#The definition below saves the JSON documents in MongoDB
def persist_in_mongo(docs):
    for doc in docs:
        election_results_collection.insert(doc)
