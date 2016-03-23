from threading import Thread
from datetime import datetime
import pymongo
from pymongo import mongo_client
from insert_data import SensorData
from insert_data import VariableYear
from insert_data import ContextData
from insert_data import ContextVariable
from insert_data import TagVariable
from insert_data import TagData
from bson.objectid import ObjectId
import insert_data
import Queue


# number of threads
WORKERS = 50
# Modify this to access a different Mongo Database
client = mongo_client.MongoClient()
database_name = "ubidots-draft-nonrel"  # database name
collection_name = "value"  # collection name
db = client.get_database(database_name)
collection = db.get_collection(collection_name)
# Number of element per cursor in mongo
batch_size = 1000


# Method run inside all created threads. consume corresponds to
# the method that uses an element of the queue q specified as
# parameter.
def worker_insert_batch(consume, q):
    while True:
        batch = q.get()
        consume(batch)
        q.task_done()


# Method used to start threads as daemons. consume is the
# method that consumes the element of the queue and target is the
# method executed in every Thread.
def start_workers_abstract(consume, target, q):
    for i in range(WORKERS):
        t = Thread(target=target, args=(consume, q,))
        t.daemon = True
        t.start()


# Initializes WORKERS Threads using the method process_batch_data as the consumer, the  worker_insert_batch
# method as the method executed during the thread and the Queue q to store the data to be consumed.
def start_workers_data(q):
    start_workers_abstract(process_batch_data, worker_insert_batch, q)


# This method performs the migration from mongo DB to Cassandra using Threads.
def create_batches_data():
    q = Queue.Queue()
    start_workers_data(q)
    get_batches_from_mongo(q)
    # collection_variables = db.get_collection("variable")
    # variables = collection_variables.find()
    # for variable in variables:
    # id_variable = str(variable['_id'])
    q.join()


# This method loads batches from mongo DB and adds them to the Queue specified
# as parameter to process them using threads
def get_batches_from_mongo(q):
    query = {}
    i = batch_size
    batch = get_batch(i - batch_size, batch_size, collection, query)
    while batch.count(True) > 0:
        q.put(batch)
        i += batch_size
        batch = get_batch(i - batch_size, batch_size, collection, query)


# Gets a SensorData object from the mongo DB record specified as parameter
def get_sensor_data_from_record(record):
    variable = str(record['variable'].id)
    value = record['value']
    t = record['timestamp']
    timestamp = t * 1000  # timestamp comes in millis should be stored in micros
    date = datetime.fromtimestamp(float(t) / 1000.0)
    year = date.year
    return SensorData(variable, year, timestamp, value)


# Gets a ContextData object from the mongo DB record specified as parameter
def get_context_data_from_record(record):
    variable = str(record['variable'].id)
    value = record['value']
    t = record['timestamp']
    timestamp = t * 1000  # timestamp comes in millis should be stored in micros
    date = datetime.fromtimestamp(float(t) / 1000.0)
    year = date.year
    result = []
    if 'context' in record:
        for key in record['context']:
            property_value = record['context'][key]
            if not isinstance(property_value, basestring):
                property_value = str(property_value)
            result.append(ContextData(variable, year, timestamp, value, key,
                                      property_value))
    return result


# Gets a ContextVariable object from the mongo DB record specified as parameter
def get_context_variable_from_record(record):
    variable = str(record['variable'].id)
    result = []
    if 'context' in record:
        for key in record['context']:
            result.append(ContextVariable(variable, str(key)))
    return result


# Gets a VariableYear object from the mongo DB record specified as parameter
def get_data_date_from_record(record):
    variable = str(record['variable'].id)
    timestamp = record['timestamp']
    date = datetime.fromtimestamp(float(timestamp) / 1000.0)
    year = date.year
    return VariableYear(variable, year)


# Gets a tuple with two elements: the first element is a list of TagVariable Objects
# and the second is a list of TagData objects. Both lists correspond to the tags of
# the the variable associated to the mongo DB record specified as parameter.
def get_tag_variable_from_record(record):
    variable = str(record['variable'].id)
    value = record['value']
    t = record['timestamp']
    timestamp = t * 1000  # timestamp comes in millis should be stored in micros
    date = datetime.fromtimestamp(float(t) / 1000.0)
    year = date.year
    variables = db.get_collection("variable")
    v = variables.find({'_id': record['variable'].id}).next()
    result = []
    result_tags = []
    if 'tags' in v:
        for tag in v['tags']:
            result_tags.append(TagData(variable, year, timestamp, value, tag))
            result.append(TagVariable(tag, variable))
    return result, result_tags


# This method gets a cursor to the value collection in Mongo DB and
# uses it to migrate the information to a Cluster with Cassandra.
def process_batch_data(batch):
    for value in batch:
        insert_data.insert_sensor_data(get_sensor_data_from_record(value))
        insert_data.insert_data_date(get_data_date_from_record(value))
        for context in get_context_data_from_record(value):
            insert_data.insert_context_data(context)
        for context_variable in get_context_variable_from_record(value):
            insert_data.insert_context_variable(context_variable)
        (tag_variable, tag_data) = get_tag_variable_from_record(value)
        for t in tag_variable:
            insert_data.insert_variable_tag(t)
            insert_data.insert_tag_variable(t)
        for t in tag_data:
            insert_data.insert_tag_data(t)


# Returns a Mongo DB cursor that performs the query specified as parameter on the collection c and
# skips the first skip elements of the query and limits the result to limit elements.
def get_batch(skip, limit, c, query):
    return c.find(query).skip(skip).limit(limit)



