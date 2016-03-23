import unittest
import insert_data
from insert_data import SensorData
from insert_data import ContextData
from insert_data import TagData
import random
import string
import uuid


class InsertDataTest(unittest.TestCase):
    def test_insert_sensor_data_batch(self):
        batches = []
        for i in range(100):
            batches.append(create_sensor_data_batch(100))
        insert_data.perform_insertion_sensor_data(batches)

    def test_insert_context_data_batch(self):
        batches = []
        for i in range(100):
            batches.append(create_context_data_batch(100))
        insert_data.perform_insertion_context_data(batches)

    def test_insert_tag_data_batch(self):
        batches = []
        for i in range(100):
            batches.append(create_tag_data_batch(100))
        insert_data.perform_insertion_tag_data(batches)

    def test_insert_sensor_data_multiple(self):
        for i in range(10000):
            s = random_sensor_data()
            insert_data.insert_sensor_data(s)
            r = insert_data.get_sensor_data_by_id(s.id_data, s.timestamp_data, s.year, s.variable)
            for x in r:
                self.assertEqual(s.variable, x.variable, "Variables should be equal")
                self.assertEqual(s.year, x.year, "Years should be equal")
                self.assertEqual(s.timestamp_data,
                                 x.timestamp_data, "Timestamps should be equal")
                self.assertEqual(s.value, x.value, "Values should be equal")

    def test_insert_context_data_multiple(self):
        for i in range(10000):
            c = random_context_data()
            insert_data.insert_context_data(c)
            r = insert_data.get_context_data_by_id(c.id_data, c.timestamp_data, c.year, c.variable, c.key,
                                                   c.property_value)
            for x in r:
                self.assertEqual(c.variable, x.variable, "Variables should be equal")
                self.assertEqual(c.year, x.year, "Years should be equal")
                self.assertEqual(c.timestamp_data,
                                 x.timestamp_data, "Timestamps should be equal")
                self.assertEqual(c.value, x.value, "Values should be equal")
                self.assertEqual(c.key, x.key, "Keys should be equal")
                self.assertEqual(c.property_value, x.property_value, "Property values should be equal")

    def test_insert_tag_data_multiple(self):
        for i in range(10000):
            t = random_tag_data()
            insert_data.insert_tag_data(t)
            r = insert_data.get_tag_data_by_id(t.id_data, t.timestamp_data, t.year, t.variable, t.tag)
            for x in r:
                self.assertEqual(t.variable, x.variable, "Variables should be equal")
                self.assertEqual(t.year, x.year, "Years should be equal")
                self.assertEqual(t.timestamp_data,
                                 x.timestamp_data, "Timestamps should be equal")
                self.assertEqual(t.value, x.value, "Values should be equal")
                self.assertEqual(t.tag, x.tag, "Tags should be equal")


def random_context_data():
    return ContextData(str(uuid.uuid4()), random.randint(1970, 9999), random.randint(0, 253402304399000000),
                       random.random() * random.randint(0, 10000000000), random_string(random.randint(1, 20)),
                       random_string(random.randint(1, 20)))


def random_sensor_data():
    return SensorData(str(uuid.uuid4()), random.randint(1970, 9999), random.randint(0, 253402304399000000),
                      random.random() * random.randint(0, 10000000000))


def random_tag_data():
    return TagData(str(uuid.uuid4()), random.randint(1970, 9999), random.randint(0, 253402304399000000),
                   random.random() * random.randint(0, 10000000000), random_string(random.randint(1, 20)))


def random_string(n):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(n))


def create_sensor_data_batch(n):
    result = []
    for i in range(n):
        result.append(random_sensor_data())
    return result


def create_context_data_batch(n):
    result = []
    for i in range(n):
        result.append(random_context_data())
    return result


def create_tag_data_batch(n):
    result = []
    for i in range(n):
        result.append(random_tag_data())
    return result


def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(InsertDataTest)
    unittest.TextTestRunner(verbosity=2).run(suite)
