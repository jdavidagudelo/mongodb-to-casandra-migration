import mongo_db
import time
t = time.time()
count = mongo_db.read_test()
print(time.time() - t)
print(count)
