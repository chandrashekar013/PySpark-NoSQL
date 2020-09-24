from starbase import Connection

c = Connection("34.217.122.102", "8000")
ratings = c.table('ratings')

if ratings.exists():
    print('dropping table ratings')
    ratings.drop()

ratings.create('rating')

ratingsFile = open("E:/BigData/Python/Spark/ml-100k/u.data", 'r')
batch = ratings.batch()

for line in ratingsFile:
    (userID, movieID, rating, timeStamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingsFile.close()
batch.commit(finalize=True)

print(ratings.fetch(2))
print(ratings.fetch(3))

ratings.drop()
