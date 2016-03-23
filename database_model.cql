CREATE KEYSPACE historic_data WITH replication =
{'class': 'NetworkTopologyStrategy', 'dc1': 3}  AND
durable_writes = true;
CREATE TABLE historic_data.sensor_data (
    variable text,
    year int,
    timestamp_data varint,
    id_data uuid,
    uuid1_data uuid,
    created_at timestamp,
    value double,
    PRIMARY KEY ((variable, year), timestamp_data, id_data, uuid1_data)
);

CREATE TABLE historic_data.tag_data (
    tag text,
    variable text,
    year int,
    timestamp_data varint,
    id_data uuid,
    uuid1_data uuid,
    created_at timestamp,
    value double,
    PRIMARY KEY ((tag, variable, year), timestamp_data, id_data, uuid1_data)
);


CREATE TABLE historic_data.context_data (
    variable text,
    key text,
    property_value text,
    year int,
    timestamp_data varint,
    id_data uuid,
    uuid1_data uuid,
    created_at timestamp,
    value double,
    PRIMARY KEY ((variable, key, year), timestamp_data, property_value, id_data, uuid1_data)
);

CREATE TABLE historic_data.tag_variable (
    tag text PRIMARY KEY,
    variable text,
    PRIMARY KEY(tag, variable)
);

CREATE TABLE historic_data.variable_tag (
    variable text,
    tag text,
    PRIMARY KEY(variable, tag)
);

CREATE TABLE historic_data.context_variable(
    variable text,
    key text,
    PRIMARY KEY (variable, key)
);

CREATE TABLE historic_data.data_date (
    variable text,
    year int,
    PRIMARY KEY (variable, year)
);
