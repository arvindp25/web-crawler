# match/spark_matcher.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
import os

DATABASE_URL = os.getenv("JDBC_DATABASE_URL")


def perform_matching_embedding():
    spark = SparkSession.builder \
        .appName("FirmableEntityMatching") \
        .getOrCreate()

    # Load ABR and Crawl data from Postgres
    abr_df = spark.read \
        .format("jdbc") \
        .option("url", DATABASE_URL) \
        .option("dbtable", "abr_records_extracted") \
        .load() \
        .select("abn", col("entity_name").alias("abr_name"))

    crawl_df = spark.read \
        .format("jdbc") \
        .option("url", DATABASE_URL) \
        .option("dbtable", "crawl_records_extracted") \
        .option("user", "admin") \
        .option("password", "admin") \
        .load() \
        .select("url", col("company_name").alias("crawl_name"))

    # Preprocessing
    tokenizer = Tokenizer(inputCol="abr_name", outputCol="abr_tokens")
    abr_df = tokenizer.transform(abr_df)
    remover = StopWordsRemover(inputCol="abr_tokens", outputCol="abr_filtered")
    abr_df = remover.transform(abr_df)

    tokenizer2 = Tokenizer(inputCol="crawl_name", outputCol="crawl_tokens")
    crawl_df = tokenizer2.transform(crawl_df)
    remover2 = StopWordsRemover(inputCol="crawl_tokens", outputCol="crawl_filtered")
    crawl_df = remover2.transform(crawl_df)

    tf = HashingTF(inputCol="abr_filtered", outputCol="abr_features", numFeatures=1000)
    abr_df = tf.transform(abr_df)

    tf2 = HashingTF(inputCol="crawl_filtered", outputCol="crawl_features", numFeatures=1000)
    crawl_df = tf2.transform(crawl_df)

    # LSH Approximate Join
    lsh = MinHashLSH(inputCol="abr_features", outputCol="abr_hashes")
    model = lsh.fit(abr_df)

    matched = model.approxSimilarityJoin(abr_df, crawl_df, 0.6, distCol="JaccardDistance") \
        .select(
            col("datasetA.abn").alias("abn"),
            col("datasetA.abr_name").alias("abr_name"),
            col("datasetB.crawl_name").alias("crawl_name"),
            col("datasetB.url").alias("url"),
            col("JaccardDistance")
        )

    matched.show(20, truncate=False)

    # Optionally write results to PostgreSQL
    matched.write \
        .format("jdbc") \
        .option("url", DATABASE_URL) \
        .option("dbtable", "matched_entities") \
        .mode("overwrite") \
        .save()

    spark.stop()
