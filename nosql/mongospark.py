from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def parseInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName(
        "mongospark").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("file:///E:/BigData/Python/Spark/ml-100k/u.user")
    # Convert it to a RDD of Row objects with (userID, age, gender, occupation, zip)
    users = lines.map(parseInput)
    # Convert that to a DataFrame
    usersDataset = spark.createDataFrame(users)

    # Write it into MongoDB
    usersDataset.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://34.217.122.102/movielens.users") \
        .mode('append') \
        .save()

    # Read it back from MongoDB into a new Dataframe
    readUsers = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://34.217.122.102/movielens.users") \
        .load()

    readUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()

    # Stop the session
    spark.stop()
