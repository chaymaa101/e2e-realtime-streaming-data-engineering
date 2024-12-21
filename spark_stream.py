import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging avec plus de détails
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")
        return s_conn
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        # Lecture du stream Kafka
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        # Log les données brutes reçues
        logger.info("Connected to Kafka topic successfully")
        return spark_df
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    try:
        # Définition du schéma correspondant à vos données
        schema = StructType([
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("post_code", StringType(), True),
            StructField("email", StringType(), True),
            StructField("username", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("registered_date", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("picture", StringType(), True)
        ])

        # Transformation du DataFrame
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        # Ajout d'une fonction de debug pour voir les données
        def debug_batch(df, epoch_id):
            logger.info(f"Processing batch {epoch_id}")
            logger.info(f"Number of records in batch: {df.count()}")
            if df.count() > 0:
                logger.info("Sample record:")
                df.show(1, truncate=False)

        # Démarrer un stream de debug
        sel.writeStream \
            .foreachBatch(debug_batch) \
            .start()

        logger.info("Selection dataframe created successfully")
        return sel
    except Exception as e:
        logger.error(f"Error creating selection DataFrame: {e}")
        return None

if __name__ == "__main__":
    # Création de la connexion Spark
    spark_conn = create_spark_connection()
    if spark_conn is None:
        logger.error("Failed to create Spark connection. Exiting.")
        exit(1)

    # Connexion à Kafka
    spark_df = connect_to_kafka(spark_conn)
    if spark_df is None:
        logger.error("Failed to connect to Kafka. Exiting.")
        exit(1)

    # Création du DataFrame de sélection
    selection_df = create_selection_df_from_kafka(spark_df)
    if selection_df is None:
        logger.error("Failed to create selection DataFrame. Exiting.")
        exit(1)

    logger.info("Starting streaming to Cassandra...")
    
    # Configuration du streaming vers Cassandra
    streaming_query = selection_df.writeStream \
        .foreachBatch(lambda df, epoch_id: df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", "spark_streaming") \
            .option("table", "created_users") \
            .save()) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down stream...")
        streaming_query.stop()