import mongo_db
import time
t = time.time()
mongo_db.create_batches_data()
print(time.time() - t)