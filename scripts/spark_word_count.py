from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .getOrCreate()

    text_file = spark.sparkContext.textFile("../data/shakespeare-big.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
                      .map(lambda word: (word.lower(), 1)) \
                      .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile("../data/shakespeare-words.txt")

