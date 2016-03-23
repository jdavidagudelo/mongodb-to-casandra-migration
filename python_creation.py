from cassandra.cluster import Cluster


cluster = Cluster()
session = cluster.connect()


def create_structure():
    session.execute("drop KEYSPACE if exists historic_data;")
    session.execute("CREATE KEYSPACE historic_data " +
                    "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")

    """
        Table sensor_data: this table stores historic data to be queried by variable, year and timestamp.
        variable: id of the variable associated to the values stored in the row.
        year: number of the year of the reading. eg. 2016, 2050.
        timestamp_data: the number of microseconds since the standard time.
        id_data: unique identifier of the record, in case two records are stored with the same timestamps.
        created_at: creation date of the record.
        value: the value read in the device.
    """
    session.execute('CREATE TABLE historic_data.sensor_data (' +
                    'variable text,' +
                    'year int,' +
                    'id_data uuid,' +
                    'uuid1_data uuid,' +
                    'timestamp_data varint,' +
                    'value double,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY ((variable, year), timestamp_data, id_data, uuid1_data)' +
                    ');')
    session.execute('CREATE TABLE historic_data.data_date(' +
                    'year int,' +
                    'variable text,' +
                    'primary key(variable, year)' +
                    ');')

    """
        Table sensor_data: this table stores historic data to be queried by variable, year and timestamp.
        variable: id of the variable associated to the values stored in the row.
        year: number of the year of the reading. eg. 2016, 2050.
        timestamp_data: the number of microseconds since the standard time.
        id_data: unique identifier of the record, in case two records are stored with the same timestamps.
        created_at: creation date of the record.
        value: the value read in the device.
        key: key associated to the context of the new record..
        property_value: value of the key associated to the context.
    """
    session.execute('CREATE TABLE historic_data.context_data (' +
                    'key text,' +
                    'year int,' +
                    'id_data uuid,' +
                    'uuid1_data uuid,' +
                    'variable text,' +
                    'timestamp_data varint,' +
                    'property_value text,' +
                    'value double,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY ((variable, key, year), timestamp_data, property_value, id_data, uuid1_data)' +
                    ');')

    """
        Table sensor_data: this table stores historic data to be queried by variable, year and timestamp.
        variable: id of the variable associated to the values stored in the row.
        year: number of the year of the reading. eg. 2016, 2050.
        timestamp_data: the number of microseconds since the standard time.
        id_data: unique identifier of the record, in case two records are stored with the same timestamps.
        created_at: creation date of the record.
        value: the value read in the device.
        tag: tag associated to the variable.
    """
    session.execute('CREATE TABLE historic_data.tag_data(' +
                    'tag text,' +
                    'year int,' +
                    'id_data uuid,' +
                    'uuid1_data uuid,' +
                    'variable text,' +
                    'timestamp_data varint,' +
                    'value double,' +
                    'created_at timestamp,' +
                    'PRIMARY KEY ((tag, variable, year), timestamp_data, id_data, uuid1_data));')
    session.execute('CREATE TABLE historic_data.tag_variable(' +
                    'tag text,' +
                    'variable text,' +
                    'PRIMARY KEY (tag, variable)' +
                    ');')
    session.execute('CREATE TABLE historic_data.variable_tag(' +
                    'tag text,' +
                    'variable text,' +
                    'PRIMARY KEY (variable, tag)' +
                    ');')
    session.execute('CREATE TABLE historic_data.context_variable(' +
                    'key text,' +
                    'variable text,' +
                    'primary key(variable, key)' +
                    ');')

