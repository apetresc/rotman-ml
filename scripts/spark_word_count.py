from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .getOrCreate()

    text_file = spark.sparkContext.textFile("gs://data-sandbox-85946/shakespeare-*.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile("gs://data-sandbox-85946/shakespeare-words")

