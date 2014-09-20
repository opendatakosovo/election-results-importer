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
    docs = create_election_result_documents(data_table)
    persist_in_mongo(docs)

    return 25


def create_data_table(csv_filepath):
    data_table = []

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        index = 0
        for row in reader:
            #print len(row)
            #print "Data at index %i:" % index
            #print row
            data_table.append(row)
            index = index + 1
        #print data_table
    return data_table


def create_election_result_documents(data_table):
    docs = []

    for column_index in range(1, len(data_table[0])):
        doc_dict = {"_id": str(ObjectId())}

        for row_index in range(0, len(data_table)):
            if row_index == 0:
                doc_dict['year'] = data_table[row_index][column_index]
            elif row_index == 1:
                doc_dict['type'] = data_table[row_index][column_index]
            else:
                doc_dict[data_table[row_index][0]] = data_table[row_index][column_index]

        print doc_dict
        print
        docs.append(doc_dict)

    return docs


#The definition below saves the JSON documents in MongoDB
def persist_in_mongo(docs):
    for doc in docs:
        election_results_collection.insert(doc)
