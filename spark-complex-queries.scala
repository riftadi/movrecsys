val ratings = spark.
    read.
    format("org.apache.spark.sql.cassandra").
    options(Map( "table" -> "ratings", "keyspace" -> "movies")).
    load()

val titles = spark.
    read.
    format("org.apache.spark.sql.cassandra").
    options(Map( "table" -> "titles", "keyspace" -> "movies")).
    load()

val tags = spark.
    read.
    format("org.apache.spark.sql.cassandra").
    options(Map( "table" -> "tags", "keyspace" -> "movies")).
    load()

val gscores = spark.
    read.
    format("org.apache.spark.sql.cassandra").
    options(Map( "table" -> "genome_scores", "keyspace" -> "movies")).
    load()

val gtags = spark.
    read.
    format("org.apache.spark.sql.cassandra").
    options(Map( "table" -> "genome_tags", "keyspace" -> "movies")).
    load()

ratings.registerTempTable("ratings")
titles.registerTempTable("titles")
tags.registerTempTable("tags")
gscores.registerTempTable("gscores")
gtags.registerTempTable("gtags")

// now we can issue SQL queries

sql("SELECT count(movieId) FROM titles").show()
sql("SELECT count(userid) FROM ratings").show()
// sql("SELECT t.movieid, t.title, avg(r.rating) rating_avg FROM titles t LEFT JOIN ratings r on t.movieid = r.movieid group by t.movieid, t.title").show()

// track the timing
import org.joda.time._

// first complex query
val t_start = DateTime.now()
val rating_tag_view = sql("SELECT DISTINCT r.movieId, r.rating, t.tag, m.genres FROM ratings r INNER JOIN tags t ON r.userId=t.userId INNER JOIN titles m ON r.movieId=m.movieId GROUP BY r.movieId, r.userId, t.tag, r.rating, m.genres HAVING r.userId=121")
rating_tag_view.createOrReplaceTempView("rating_tag_view")

val gt_gs_view = sql("SELECT DISTINCT gt.tag, gs.relevance FROM gtags gt INNER JOIN gscores gs ON gt.tagId=gs.tagId GROUP BY gt.tag, gs.relevance, gt.tagId")
gt_gs_view.createOrReplaceTempView("gt_gs_view")

val relevant_view = sql("SELECT t.movieId, t.rating, t.tag, gt.relevance FROM gt_gs_view gt INNER JOIN rating_tag_view t ON t.tag = gt.tag GROUP BY t.movieId, t.genres, t.rating, t.tag, gt.relevance HAVING gt.relevance > 0.7")
relevant_view.createOrReplaceTempView("relevant_view")

sql("SELECT DISTINCT m.movieId, m.title FROM relevant_view r INNER JOIN titles m ON r.movieId=m.movieId ORDER BY m.movieId").show()
val t_end = DateTime.now()
new Period(t_start, t_end).toStandardSeconds()

// second complex query
val t_start = DateTime.now()
sql("SELECT avg(r.rating) FROM titles t JOIN ratings r ON t.movieid=r.movieid WHERE t.title LIKE '%War%' AND r.timestamp >= 1262304000 AND r.timestamp <= 1451606399").show()
val t_end = DateTime.now()
new Period(t_start, t_end).toStandardSeconds()

// third complex query
val t_start = DateTime.now()
val reco = sql("SELECT r.movieId, m.genres, r.userId, r.rating, r.timestamp FROM ratings r INNER JOIN titles m ON m.movieId=r.movieId WHERE r.timestamp > 1356998400 AND r.userId = 69322 AND r.rating > 3 ORDER BY r.timestamp DESC LIMIT 20")

reco.createOrReplaceTempView("reco")
// reco view gives same result as postgre

sql("SELECT DISTINCT m.title, m.genres FROM titles m INNER JOIN reco r ON split(m.genres,'|')[0]=split(r.genres,'|')[0] LIMIT 10").show()
val t_end = DateTime.now()
new Period(t_start, t_end).toStandardSeconds()


