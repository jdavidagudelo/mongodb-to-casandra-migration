//Get historic data by variable, or timestamp
//date field used to avoid partitions to overflow the maximum 
//number of columns allowed by cassandra
CREATE TABLE historic_data.sensor_data (
    variable text,
	id uuid,
	year int,
    timestamp timestamp,
    value float,
	created_at timestamp,
    PRIMARY KEY ((variable, year), timestamp, id)
);

//table used to get existing dates with records of a variable
CREATE TABLE historic_data.data_date(
	year int,
	variable text,
	primary key(variable, year)
);

//Get historic data by context info
CREATE TABLE historic_data.context_data (
    key text,
    year int,
	id uuid,
    variable text,
    timestamp timestamp,
    property_value text,
	value float,
	created_at timestamp,
    PRIMARY KEY ((variable, key, property_value, year), timestamp, id)
);

//get historic data by tag
CREATE TABLE historic_data.tag_data(
	tag text, 
	year int,
	id uuid, 
	variable text, 
	timestamp timestamp,
	value float, 
	created_at timestamp, 
	PRIMARY KEY ((tag, variable, year), timestamp, id)
);

//get variables by tag
CREATE TABLE historic_data.tag_variable(
	tag text, 
	variable text, 
	PRIMARY KEY (tag)
);

CREATE TABLE historic_data.variable_tag(
	tag text, 
	variable text, 
	PRIMARY KEY (variable)
);

//variable basic info
CREATE TABLE historic_data.variable(
	id text,
	name text,
	unit text,
	description text,
	created_at timestamp,
	PRIMARY KEY (id)
);
