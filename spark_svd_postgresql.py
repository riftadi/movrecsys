from pyspark import SQLContext
from pyspark.mllib.recommendation import ALS
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from time import time

sqlContext = SQLContext(sc)

# replace with postgresql connection
CLOUDSQL_INSTANCE_IP = "<IP PostgreSQL>"
CLOUDSQL_DB_NAME = "<DB NAME>"
CLOUDSQL_USER = "<DB USERNAME>"
CLOUDSQL_PWD = "<DB PASSWORD>"

jdbcDriver = "org.postgresql.Driver"
jdbcUrl = 'jdbc:postgresql://%s:5432/%s?user=%s&password=%s' % (
    CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)

TABLE_RATINGS = 'RATINGS'
TABLE_MOVIES = 'MOVIES'
TABLE_RECOMMENDATIONS = 'RECOMMENDATIONS'

t0 = time()
print "Reading data from postgresql..."
dfRates = sqlContext.read.format('jdbc') \
    .option('useSSL', False) \
    .option("url", jdbcUrl) \
    .option("dbtable", TABLE_RATINGS) \
    .option("driver", jdbcDriver) \
    .load()

dfRates.registerTempTable('Ratings')
sqlContext.cacheTable('Ratings')
tt = time() - t0
print "Data is loaded in %s seconds" % round(tt,3)

rank = 8
seed = 5L
iterations = 10
regularization_parameter = 0.1

t0 = time()
print "Training the ALS model..."
model = ALS.train(dfRates.rdd.map(lambda r: (int(r[0]), int(r[1]), r[2])).cache(), rank=rank, seed=seed,
                  iterations=iterations, lambda_=regularization_parameter)
tt = time() - t0
print "ALS model built!"
print "New model trained in %s seconds" % round(tt,3)

predictions = model.recommendProductsForUsers(10) \
    .flatMap(lambda pair: pair[1]) \
    .map(lambda rating: (rating.user, rating.product, rating.rating))

schema = StructType([StructField("userId", StringType(), True), StructField("movieId", StringType(), True),
                     StructField("prediction", FloatType(), True)])

dfToSave = sqlContext.createDataFrame(predictions, schema)
#dfToSave.write.jdbc(url=jdbcUrl, table=TABLE_RECOMMENDATIONS)
t0 = time()
dfToSave.write.option('driver', 'org.postgresql.Driver').jdbc(jdbcUrl, TABLE_RECOMMENDATIONS, mode='overwrite')
tt = time() - t0

print "Recommendation saved to DB in %s seconds" % round(tt,3)