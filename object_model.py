from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
import uuid
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime


class SensorData(Model):
    variable = columns.Text(primary_key=True, partition_key=True)
    year = columns.Integer(primary_key=True, partition_key=True)
    timestamp_data = columns.DateTime(primary_key=True)
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    value = columns.Float()
    created_at = columns.DateTime()


auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
connection.setup(['127.0.0.1'], "test", protocol_version=3, auth_provider=auth_provider)
sync_table(SensorData)
r = SensorData.create(variable='v1', year=2016, timestamp_data=1900001, value=10.01, created_at=datetime.now())


def insert_sensor_data(data):
    return SensorData.create(variable=data.variable, year=data.year, timestamp_data=data.timestamp_data,
                             value=data.value, created_at=datetime.now())