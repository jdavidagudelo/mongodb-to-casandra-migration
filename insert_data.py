from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from threading import Thread
import Queue
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
import uuid

# in case authentication is required for the cluster
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
# Number of threads
WORKERS = 50
# Modify this to access a different cassandra cluster
cluster = Cluster()
session = cluster.connect()


class SensorData:
    def __init__(self, variable, year, timestamp_data, value,
                 id_data=None, created_at=None, uuid1_data=None):
        self.variable = variable
        self.year = year
        self.timestamp_data = timestamp_data
        self.value = value
        if id_data is None:
            self.id_data = uuid.uuid4()
        else:
            self.id_data = id_data
        if uuid1_data is None:
            self.uuid1_data = uuid.uuid1()
        else:
            self.uuid1_data = uuid1_data
        if created_at is None:
            self.created_at = datetime.now()
        else:
            self.created_at = created_at


class ContextData(SensorData):
    def __init__(self, variable, year, timestamp_data, value, key, property_value, id_data=None,
                 created_at=None, uuid1_data=None):
        SensorData.__init__(self, variable, year, timestamp_data, value, id_data, created_at, uuid1_data
                            )
        self.key = key
        self.property_value = property_value


class TagData(SensorData):
    def __init__(self, variable, year, timestamp_data, value, tag, id_data=None, created_at=None, uuid1_data=None):
        SensorData.__init__(self, variable, year, timestamp_data, value, id_data, created_at, uuid1_data)
        self.tag = tag


class TagVariable:
    def __init__(self, tag, variable):
        self.tag = tag
        self.variable = variable


class ContextVariable:
    def __init__(self, variable, key):
        self.key = key
        self.variable = variable


class VariableYear:
    def __init__(self, variable, year):
        self.variable = variable
        self.year = year


class SensorDataDao:
    def __init__(self):
        self.session = session
        self.insert_sensor_data_query = insert_sensor_data_query()

    def insert_sensor_data(self, data):
        self.session.execute(self.insert_sensor_data_query, (data.variable, data.year, data.id_data,
                                                             data.timestamp_data, data.value, data.created_at,
                                                             data.uuid1_data))

    def get_sensor_data_by_id(self, year, variable):
        query = "select * from historic_data.sensor_data where" \
            " year = ? and variable = ?;"
        prepared = self.session.prepare(query)
        r = session.execute(prepared, (year, variable))
        result = []
        for x in r:
            result.append(SensorData(x.variable, x.year, x.timestamp_data, x.value,
                                     x.id_data, x.created_at, x.uuid1_data))
        return result


class ContextDataDao:
    def __init__(self):
        self.session = session
        self.insert_context_data_query = insert_context_data_query()

    def insert_context_data(self, data):
        self.session.execute(self.insert_context_data_query, (
            data.variable, data.key, data.property_value, data.year, data.id_data,
            data.timestamp_data, data.value, data.created_at, data.uuid1_data))


class TagDataDao:
    def __init__(self):
        self.session = session
        self.insert_tag_data_query = insert_tag_data_query()

    def insert_tag_data(self, data):
        self.session.execute(self.insert_tag_data_query,
                             (
                                 data.variable, data.tag, data.year, data.id_data, data.timestamp_data, data.value,
                                 data.created_at,
                                 data.uuid1_data))


class VariableYearDao:
    def __init__(self):
        self.session = session
        self.insert_data_date_query = insert_data_date_query()

    def insert_data_date(self, data):
        self.session.execute(self.insert_data_date_query, (data.variable, data.year))

    def get_all_variable_year(self):
        r = self.session.execute("select * from historic_data.data_date;")
        result = []
        for x in r:
            result.append(VariableYear(x.variable, x.year))
        return result


class ContextVariableDao:
    def __init__(self):
        self.session = session
        self.insert_context_variable_query = insert_context_variable_query()

    def insert_context_variable(self, data):
        self.session.execute(self.insert_context_variable_query, (data.key, data.variable))


class TagVariableDao:
    def __init__(self):
        self.session = session
        self.insert_tag_variable_query = insert_tag_variable_query()
        self.insert_variable_tag_query = insert_variable_tag_query()

    def insert_tag_variable(self, data):
        self.session.execute(self.insert_tag_variable_query, (data.tag, data.variable))

    def insert_variable_tag(self, data):
        self.session.execute(self.insert_variable_tag_query, (data.tag, data.variable))


def get_all_sensor_data():
    r = session.execute("select * from historic_data.sensor_data")
    result = []
    for x in r:
        result.append(SensorData(x.variable, x.year, x.timestamp_data, x.value, x.id_data, x.created_at))
    return result


def get_sensor_data_by_id(id_data, timestamp_data, year, variable):
    query = "select * from historic_data.sensor_data where id_data = ? and timestamp_data = ? and" \
            " year = ? and variable = ?"
    prepared = session.prepare(query)
    return session.execute(prepared, (id_data, timestamp_data, year, variable))


def insert_data_batch(data, prepared, get_params):
    batch = BatchStatement()
    for d in data:
        batch.add(prepared, get_params(d))
    session.execute(batch)


# prepared query to insert sensor_data and avoid sql injection
def insert_sensor_data_query():
    query = "insert into historic_data.sensor_data(variable, year, id_data, timestamp_data, value," \
            " created_at, uuid1_data) " \
            "values(?,?,?,?,?,?,?);"
    return session.prepare(query)


# data is a list of objects of type SensorData
# all elements in data are inserted in batch to cassandra database
def insert_sensor_data_batch(data):
    insert_data_batch(data, insert_sensor_data_query(), lambda d: (d.variable, d.year, d.id_data,
                                                                   d.timestamp_data, d.value, d.created_at,
                                                                   d.uuid1_data))


# data is an object of type SensorData
def insert_sensor_data(data):
    session.execute(insert_sensor_data_query(), (data.variable, data.year, data.id_data,
                                                 data.timestamp_data, data.value, data.created_at, data.uuid1_data))


def get_context_data_by_id(id_data, timestamp_data, year, variable, key, property_value):
    query = "select * from historic_data.context_data where id_data = ? and timestamp_data = ? and" \
            " year = ? and variable = ? and key = ? and property_value = ?"
    prepared = session.prepare(query)
    return session.execute(prepared, (id_data, timestamp_data, year, variable, key, property_value))


#  prepared query to insert context_data
def insert_context_data_query():
    query = "insert into historic_data.context_data(variable, key,property_value, year, id_data, " \
            "timestamp_data,value,created_at, uuid1_data)"
    query += " values(?,?,?,?,?,?,?,?,?);"
    return session.prepare(query)


# data is a list of objects of type ContextData
# all elements in data are inserted in batch to cassandra database
def insert_context_data_batch(data):
    insert_data_batch(data, insert_context_data_query(),
                      lambda d: (d.variable, d.key, d.property_value, d.year, d.id_data,
                                 d.timestamp_data, d.value, d.created_at, d.uuid1_data))


# data is an object of type ContextData
def insert_context_data(data):
    session.execute(insert_context_data_query(), (
        data.variable, data.key, data.property_value, data.year, data.id_data,
        data.timestamp_data, data.value, data.created_at, data.uuid1_data))


def get_tag_data_by_id(id_data, timestamp_data, year, variable, tag):
    query = "select * from historic_data.tag_data where id_data = ? and timestamp_data = ? and" \
            " year = ? and variable = ? and tag = ?"
    prepared = session.prepare(query)
    return session.execute(prepared, (id_data, timestamp_data, year, variable, tag))


def insert_tag_data_query():
    query = "insert into historic_data.tag_data(variable, tag, year, id_data, timestamp_data, value, " \
            "created_at, uuid1_data) "
    query += "values(?,?,?,?,?,?,?,?);"
    return session.prepare(query)


def insert_tag_data_batch(data):
    insert_data_batch(data, insert_tag_data_query(), lambda d: (d.variable, d.tag, d.year, d.id_data,
                                                                d.timestamp_data, d.value, d.created_at, d.uuid1_data))


# data is a dictionary that contains the keys variable, tag, year, timestamp and value,
# the id and created_at fields are automatically generated by cassandra
def insert_tag_data(data):
    session.execute(insert_tag_data_query(),
                    (
                        data.variable, data.tag, data.year, data.id_data, data.timestamp_data, data.value,
                        data.created_at,
                        data.uuid1_data))


def insert_data_date_query():
    query = "insert into historic_data.data_date(variable, year) values(?, ?);"
    return session.prepare(query)


def insert_data_date_batch(data):
    insert_data_batch(data, insert_data_date_query(), lambda d: (d.variable, d.year))


# data is a dictionary that contains the keys
def insert_data_date(data):
    session.execute(insert_data_date_query(), (data.variable, data.year))


def insert_context_variable_query():
    query = "insert into historic_data.context_variable(key, variable) values(?, ?);"
    return session.prepare(query)


def insert_context_variable(data):
    session.execute(insert_context_variable_query(), (data.key, data.variable))


def insert_tag_variable_query():
    query = "insert into historic_data.tag_variable(tag, variable) values(?, ?);"
    return session.prepare(query)


def insert_tag_variable_batch(data):
    insert_data_batch(data, insert_tag_variable_query(), lambda d: (d.tag, d.variable))


def insert_tag_variable(data):
    session.execute(insert_tag_variable_query(), (data.tag, data.variable))


def insert_variable_tag_query():
    query = "insert into historic_data.variable_tag(tag, variable) values(?, ?);"
    return session.prepare(query)


def insert_variable_tag_batch(data):
    insert_data_batch(data, insert_variable_tag_query(), lambda d: (d.tag, d.variable))


def insert_variable_tag(data):
    session.execute(insert_variable_tag_query(), (data.tag, data.variable))


# This is the default worker to insert data
# consume is a function that inserts data to the database
# and q is a queue that servers the elements to be inserted
def worker_insert_batch(consume, q):
    while True:
        batch = q.get()
        consume(batch)
        q.task_done()


# start WORKERS workers as daemons to perform a task.
# target is the worker function that will be
# executed as a thread and consume is the function used to insert data
# to a database
def start_workers_abstract(consume, target, q):
    for i in range(WORKERS):
        t = Thread(target=target, args=(consume, q,))
        t.daemon = True
        t.start()


# starts WORKERS Threads as daemons used
# to insert data to the sensor_data table
def start_workers_insert_sensor_data_batch(q):
    start_workers_abstract(insert_sensor_data_batch, worker_insert_batch, q)


# starts WORKERS Threads as daemons used
# to insert data to the context_data table
def start_workers_insert_context_data_batch(q):
    start_workers_abstract(insert_context_data_batch, worker_insert_batch, q)


# starts WORKERS Threads as daemons used
# to insert data to the tag_data table
def start_workers_insert_tag_data_batch(q):
    start_workers_abstract(insert_tag_data_batch, worker_insert_batch, q)


def start_workers_insert_data_date_batch(q):
    start_workers_abstract(insert_data_date_batch, worker_insert_batch, q)


def start_workers_insert_tag_variable_batch(q):
    start_workers_abstract(insert_tag_variable_batch, worker_insert_batch, q)


def start_workers_insert_variable_tag_batch(q):
    start_workers_abstract(insert_tag_variable_batch, worker_insert_batch, q)


# fills the queue to serve data to the workers
def perform_insertion(batches, q):
    for batch in batches:
        q.put(batch)
    q.join()


# Performs insertion of data to the sensor_data
# table using Threads
def perform_insertion_sensor_data(batches):
    q = Queue.Queue()
    start_workers_insert_sensor_data_batch(q)
    perform_insertion(batches, q)


# Performs insertion of data to the context_data
# table using Threads
def perform_insertion_context_data(batches):
    q = Queue.Queue()
    start_workers_insert_context_data_batch(q)
    perform_insertion(batches, q)


# Performs insertion of data to the sensor_data
# table using Threads
def perform_insertion_tag_data(batches):
    q = Queue.Queue()
    start_workers_insert_tag_data_batch(q)
    perform_insertion(batches, q)


def perform_insertion_data_date(batches):
    q = Queue.Queue()
    start_workers_insert_data_date_batch(q)
    perform_insertion(batches, q)


def perform_insertion_tag_variable(batches):
    q = Queue.Queue()
    start_workers_insert_tag_variable_batch(q)
    perform_insertion(batches, q)


def perform_insertion_variable_tag(batches):
    q = Queue.Queue()
    start_workers_insert_variable_tag_batch(q)
    perform_insertion(batches, q)
