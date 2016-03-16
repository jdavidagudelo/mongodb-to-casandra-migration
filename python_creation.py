from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

def create_structure():
    session.execute("CREATE KEYSPACE historic_data " +
                    "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
    session.execute('CREATE TABLE historic_data.sensor_data (' +
                    'variable text,' +
                    'year int,' +
                    'id uuid,' +
                    'timestamp timestamp,' +
                    'value float,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY ((variable, year), timestamp, id)' +
                    ');')
    session.execute('CREATE TABLE historic_data.data_date(' +
                    'year int,' +
                    'variable text,' +
                    'primary key(variable, year)' +
                    ');')
    session.execute('CREATE TABLE historic_data.context_data (' +
                    'key text,' +
                    'year int,' +
                    'id uuid,' +
                    'variable text,' +
                    'timestamp timestamp,' +
                    'property_value text,' +
                    'value float,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY ((variable, key, property_value, year), timestamp, id)' +
                    ');')
    session.execute('CREATE TABLE historic_data.tag_data(' +
                    'tag text,' +
                    'year int,' +
                    'id uuid,' +
                    'variable text,' +
                    'timestamp timestamp,' +
                    'value float,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY ((tag, variable, year), timestamp, id));')
    session.execute('CREATE TABLE historic_data.tag_variable(' +
                    'tag text,' +
                    'variable text,' +
                    'PRIMARY KEY (tag)' +
                    ');')
    session.execute('CREATE TABLE historic_data.variable_tag(' +
                    'tag text,' +
                    'variable text,' +
                    'PRIMARY KEY (variable)' +
                    ');')
    session.execute('CREATE TABLE historic_data.variable(' +
                    'id text,' +
                    'name text,' +
                    'unit text,' +
                    'description text,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY (id)' +
                    ');')

