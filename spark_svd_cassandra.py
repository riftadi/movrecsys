from pyspark.mllib.recommendation import ALS
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
from time import time
from pyspark.sql.types import StructType, StructField, StringType, FloatType

t0 = time()
dfRates = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="ratings", keyspace="movies")\
    .load()
tt = time() - t0
print "Data is loaded in %s seconds" % round(tt,3)

rank = 8
seed = 5L
iterations = 10
regularization_parameter = 0.1

print "Training the ALS model..."

t0 = time()
model = ALS.train(dfRates.rdd.map(lambda r: (int(r[0]), int(r[1]), r[2])).cache(), rank=rank, seed=seed,
                  iterations=iterations, lambda_=regularization_parameter)
tt = time() - t0

print "New model trained in %s seconds" % round(tt,3)

predictions = model.recommendProductsForUsers(10) \
    .flatMap(lambda pair: pair[1]) \
    .map(lambda rating: (rating.user, rating.product, rating.rating))

dfToSave = predictions.toDF(["userid", "movieid", "prediction"])

t0 = time()
options={"table":"recommendations", "keyspace":"movies", "confirm.truncate":"true"}
dfToSave.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('overwrite')\
    .options(**options)\
    .save()
tt = time() - t0

print "Recommendation saved to DB in %s seconds" % round(tt,3)