from starbase import Connection

c = Connection("34.217.122.102", "8000")
ratings = c.table('ratings')

# check if Table Ratings exists in Hbase
if ratings.exists():
    print('dropping table ratings')
    ratings.drop()

# Create Rating table
ratings.create('rating')

# Get data from HDFS
ratingsFile = open("E:/BigData/Python/Spark/ml-100k/u.data", 'r')

# Create a batch and insert the data into Hbase
batch = ratings.batch()

for line in ratingsFile:
    (userID, movieID, rating, timeStamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingsFile.close()
batch.commit(finalize=True)

# Fetch the data from Hbase post insertion
print(ratings.fetch(2))
print(ratings.fetch(3))

ratings.drop()
