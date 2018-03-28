from pyspark.sql import SparkSession, Row, functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
  spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

  # Definindo os schemas para as tabelas 
  ratingsSchema = StructType([
	StructField("userId", IntegerType(), True),
	StructField("movieId", IntegerType(), True),
	StructField("rating", IntegerType(), True)])

  occupationSchema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("occupation", StringType(), False)])

  # Dataframes
  ratingsDataframe = spark.read.load("hdfs:///user/maria_dev/ml-100k/u.data", format="csv", sep="\t", schema=ratingsSchema)
  ratingsDataframe.createOrReplaceTempView("ratings")

  occupationsDataframe = spark.read.load("hdfs:///user/maria_dev/ml-100k/u.user", format="csv", sep="|", schema=occupationSchema)
  occupationsDataframe.createOrReplaceTempView("occupations")

  #spark.sql("select * from ratings limit 10").show()
  #spark.sql("select * from occupations limit 10").show()

  # Realizando a consulta com o Spark SQL. Melhor impossivel :)
  spark.sql("select o.occupation, avg(r.rating) rating from ratings r inner join occupations o on r.userid = o.userId group by o.occupation order by o.occupation").show()

  # Stop the session
  spark.stop()
