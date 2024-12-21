# import logging
# from datetime import datetime

# from cassandra.cluster import Cluster
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, StructField, StringType

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[logging.StreamHandler()]
# )

# logger = logging.getLogger(__name__)

# def create_keyspace(session):
#     session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS spark_streaming
#         WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
#         """)
    
#     logger.info("Keyspace created successfully")

# def create_table(session):
#     session.execute("""
#         CREATE TABLE IF NOT EXISTS spark_streaming.created_users (
#             first_name TEXT,
#             last_name TEXT,
#             gender TEXT,
#             address TEXT,
#             post_code TEXT,
#             email TEXT,
#             username TEXT PRIMARY KEY,
#             dob TEXT,
#             registered_date TEXT,
#             phone TEXT,
#             picture TEXT);
#         """)
    
#     logger.info("Table created successfully")

# # def insert_data(session, **kwargs):
# #     logger.info("Inserting data")

# #     first_name = kwargs.get('first_name')
# #     last_name = kwargs.get('last_name')
# #     gender = kwargs.get('gender')
# #     address = kwargs.get('address')
# #     postcode = kwargs.get('post_code')
# #     email = kwargs.get('email')
# #     username = kwargs.get('username')
# #     dob = kwargs.get('dob')
# #     registered_date = kwargs.get('registered_date')
# #     phone = kwargs.get('phone')
# #     picture = kwargs.get('picture')

# #     try:
# #         session.execute("""
# #             INSERT INTO spark_streaming.created_users (first_name, last_name, gender, address,
# #                         post_code, email, username, dob, registered_date, phone, picture)
# #                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# #         """, (first_name, last_name, gender, address,
# #               postcode, email, username, dob, registered_date, phone, picture))
# #         logger.info(f"Data inserted for {first_name} {last_name}")

# #     except Exception as e:
# #         logger.error(f"Error while inserting data: {e}")

# def insert_data(session, **kwargs):
#     logger.info("Inserting data")

#     # Récupération des données
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     # Vérifier les champs requis
#     if not username or not first_name or not last_name:
#         logger.error(f"Missing required fields: username={username}, first_name={first_name}, last_name={last_name}")
#         return

#     try:
#         session.execute("""
#             INSERT INTO spark_streaming.created_users (first_name, last_name, gender, address,
#                     post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logger.info(f"Data inserted for {first_name} {last_name}")

#     except Exception as e:
#         logger.error(f"Error while inserting data: {e}")


# def create_spark_connection():
#     s_conn = None

#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
#                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
#             .config('spark.cassandra.connection.host', 'cassandra_db') \
#             .getOrCreate()
#         s_conn.sparkContext.setLogLevel("ERROR")
#         logger.info("Spark connection created successfully")

#     except Exception as e:
#         logger.error(f"Error while creating spark connection: {e}")
    
#     return s_conn

# def connect_to_kafka(spark_conn):
#     spark_df = None
#     try:
#         spark_df = spark_conn.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'broker:9092') \
#             .option('subscribe', 'users_created') \
#             .option('startingOffsets', 'earliest') \
#             .load()
#         logger.info("Kafka dataframe created successfully")
#     except Exception as e:
#         logger.error(f"Kafka dataframe could not be created because: {e}")
#         logger.error(f"Error type: {type(e).__name__}")
#         logger.error(f"Error details: {str(e)}")
#         import traceback
#         logger.error(f"Traceback: {traceback.format_exc()}")
    
#     if spark_df is None:
#         logger.error("Failed to create Kafka dataframe")
    
#     return spark_df

# def create_cassandra_connection():
#     try:
#         # Connection to Cassandra cluster
#         cluster = Cluster(['cassandra_db'])
#         cas_session = cluster.connect()
#         logger.info("Cassandra connection created successfully")
#         return cas_session
    
#     except Exception as e:
#         logger.error(f"Error while creating Cassandra connection: {e}")
#         return None

# # def create_selection_df_from_kafka(spark_df):
# #     schema = StructType([
# #         StructField("first_name", StringType(), False),
# #         StructField("last_name", StringType(), False),
# #         StructField("gender", StringType(), False),
# #         StructField("address", StringType(), False),
# #         StructField("post_code", StringType(), False),
# #         StructField("email", StringType(), False),
# #         StructField("username", StringType(), False),
# #         StructField("dob", StringType(), False),
# #         StructField("registered_date", StringType(), False),
# #         StructField("phone", StringType(), False),
# #         StructField("picture", StringType(), False)
# #     ])

# #     sel = spark_df.selectExpr("CAST(value AS STRING)") \
# #         .select(from_json(col("value"), schema).alias("data")) \
# #         .select("data.*")
# #     logger.info("Selection dataframe created successfully")
# #     return sel

# def create_selection_df_from_kafka(spark_df):
#     schema = StructType([
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("dob", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])

#     sel = spark_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), schema).alias("data")) \
#         .select("data.*") \
#         .filter(col("username").isNotNull())  # Filtrer les lignes avec username == null

#     logger.info("Selection dataframe created successfully")
#     return sel



# if __name__ == "__main__":
#     # Create Spark connection
#     spark_conn = create_spark_connection()

#     if spark_conn is not None:
#         # Create connection to Kafka with Spark
#         spark_df = connect_to_kafka(spark_conn)
#         selection_df = create_selection_df_from_kafka(spark_df)

#         logger.info("Selection dataframe schema:")
#         selection_df.printSchema()

#         # Create Cassandra connection
#         session = create_cassandra_connection()
        
#         if session is not None:
#             create_keyspace(session)
#             create_table(session)

#             # Insert data into Cassandra
#             insert_data(session)

#             streaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
#                     .option('keyspace', 'spark_streaming', ) \
#                     .option('checkpointLocation', '/tmp/checkpoint') \
#                     .option('table', 'created_users') \
#                     .start()
            
#             streaming_query.awaitTermination()
#             session.shutdown()




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