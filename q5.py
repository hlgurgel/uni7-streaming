from pyspark.sql import SparkSession, Row, functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, IntegerType

if __name__ == "__main__":
  spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

  # Definindo os schemas para as tabelas 
  ratingsSchema = StructType([
	StructField("userId", IntegerType(), True),
	StructField("movieId", IntegerType(), True),
	StructField("rating", IntegerType(), True)])

  itemSchema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("movieTitle", StringType(), True),
    StructField("releaseDate", StringType(), True),
    StructField("videoReleaseDate", StringType(), True),
    StructField("imdb", StringType(), True),
    StructField("unknown", IntegerType(), True),
    StructField("Action", IntegerType(), True),
    StructField("Adventure", IntegerType(), True),
    StructField("Animation", IntegerType(), True),
    StructField("Childrens", IntegerType(), True),
    StructField("Comedy", IntegerType(), True),
    StructField("Crime", IntegerType(), True),
    StructField("Documentary", IntegerType(), True),
    StructField("Drama", IntegerType(), True),
    StructField("Fantasy", IntegerType(), True),
    StructField("FilmNoir", IntegerType(), True),
    StructField("Horror", IntegerType(), True),
    StructField("Musical", IntegerType(), True),
    StructField("Mystery", IntegerType(), True),
    StructField("Romance", IntegerType(), True),
    StructField("SciFi", IntegerType(), True),
    StructField("Thriller", IntegerType(), True),
    StructField("War", IntegerType(), True),
    StructField("Western", IntegerType(), True)])

  # Dataframes
  spark.read.load("hdfs:///user/maria_dev/ml-100k/u.data", format="csv", sep="\t", schema=ratingsSchema).createOrReplaceTempView("data")
  spark.read.load("hdfs:///user/maria_dev/ml-100k/u.item", format="csv", sep="|", schema=itemSchema).createOrReplaceTempView("item")

  # Realizando a consulta com o Spark SQL. Melhor impossivel :)
  spark.sql("select data.rating, \
    item.unknown,\
    item.Action,\
    item.Adventure,\
    item.Animation,\
    item.Childrens,\
    item.Comedy,\
    item.Crime,\
    item.Documentary,\
    item.Drama,\
    item.Fantasy,\
    item.FilmNoir,\
    item.Horror,\
    item.Musical,\
    item.Mystery,\
    item.Romance,\
    item.SciFi,\
    item.Thriller,\
    item.War,\
    item.Western\
    from data inner join item on data.movieId = item.movieId").createOrReplaceTempView("final")

  # Calculando a media com UNPIVOT
  spark.sql("\
    select rating, 'unknown' category from final where unknown = 1\
    union all\
    select rating, 'Action' category from final where Action = 1\
    union all\
    select rating, 'Adventure' category from final where Adventure = 1\
    union all\
    select rating, 'Animation' category from final where Animation = 1\
    union all\
    select rating, 'Childrens' category from final where Childrens = 1\
    union all\
    select rating, 'Comedy' category from final where Comedy = 1\
    union all\
    select rating, 'Crime' category from final where Crime = 1\
    union all\
    select rating, 'Documentary' category from final where Documentary = 1\
    union all\
    select rating, 'Drama' category from final where Drama = 1\
    union all\
    select rating, 'Fantasy' category from final where Fantasy = 1\
    union all\
    select rating, 'FilmNoir' category from final where FilmNoir = 1\
    union all\
    select rating, 'Horror' category from final where Horror = 1\
    union all\
    select rating, 'Musical' category from final where Musical = 1\
    union all\
    select rating, 'Mystery' category from final where Mystery = 1\
    union all\
    select rating, 'Romance' category from final where Romance = 1\
    union all\
    select rating, 'SciFi' category from final where SciFi = 1\
    union all\
    select rating, 'Thriller' category from final where Thriller = 1\
    union all\
    select rating, 'War' category from final where War = 1\
    union all\
    select rating, 'Western' category from final where Western = 1").createOrReplaceTempView("final")

  spark.sql("select category, avg(rating) from final group by category").show()

  # Stop the session
  spark.stop()
