# TRAITEMENT SPARK ROBUSTE AVEC GESTION DES NULLS
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_date,current_date ,lit,substring
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
import os
project_root = os.getcwd()               
output_dir   = os.path.join(project_root, "cleaned_cdrs")
def is_valid_number(col_name: str):
    pattern = r"""^(
        212[67]\d{8}|
        1\d{10}|
        44\d{10}|
        49\d{11}|
        61\d{9}|
        86\d{11}|
        91\d{10}|
        351\d{9}|
        55\d{11}|
        32\d{9}|
        90\d{10}|
    )$"""
    # on enlève les espaces pour Spark
    pattern = pattern.replace(" ", "").replace("\n", "")
    return col(col_name).rlike(pattern) 

def clean_voice_df(df):
    return df.dropna(subset=["caller_id"]) \
             .filter(col("duration_sec") > 0).filter( is_valid_number("caller_id"))\
             .fillna({
                 'cell_id': 'UNK',
                 'technology': '4G', # Valeur par défaut
                 'callee_id': 'UNK'
             }) \
              .withColumn("technology",
                 when(col("technology") == "LTE", "4G")
                 .when(col("technology") == "NR", "5G")
                 .when(col("technology") == "UMTS", "3G")
                 .otherwise(col("technology"))  
             ) \
              .withColumn("callee_id",
                when(~is_valid_number("callee_id"),"UNK")
                .otherwise(col("callee_id"))
             )\
             .withColumn("rating_status",lit("ready")) \
             .withColumn("callee_cc", substring(col("callee_id"), 1, 3)) \
             .withColumn("product_code",
                when(col("callee_cc")=="UNK","VOICE_NAT")
                .when(col("callee_cc") != "212", "VOICE_INT")
                .otherwise("VOICE_NAT")
             ) \
             .drop("callee_cc")

def clean_sms_df(df):
    return df.dropna(subset=["sender_id"]) \
             .fillna({
                 'cell_id': 'UNK',
                 'technology': '4G',
                 'receiver_id' :'UNK'
             }) \
             .filter( is_valid_number("sender_id")) \
             .withColumn("technology",
                 when(col("technology") == "LTE", "4G")
                 .when(col("technology") == "NR", "5G")
                 .when(col("technology") == "UMTS", "3G")
                 .otherwise(col("technology"))  
             ) \
             .withColumn("receiver_id",
                when(~is_valid_number("receiver_id"),"UNK")
                .otherwise(col("receiver_id"))
            ) \
             .withColumn("rating_status",lit("ready")) \
             .withColumn("product_code",lit("SMS_STD"))

def clean_data_df(df):
    fallback = "UNK"
    return df.dropna(subset=["user_id"]) \
             .filter( is_valid_number("user_id"))\
             .fillna({
                 'cell_id': 'UNK',
                 'technology': '4G'
             }) \
             .withColumn("technology",
                 when(col("technology") == "LTE", "4G")
                 .when(col("technology") == "NR", "5G")
                 .when(col("technology") == "UMTS", "3G")
                 .otherwise(col("technology")) 
             ) \
             .withColumn("data_volume_mb_imputed",
              when(col("data_volume_mb").isNull(), lit(fallback))
              .otherwise(col("data_volume_mb"))
             ) \
             .withColumn("rating_status",
               when(
                ( col("data_volume_mb").isNull() ) |
                ( col("session_duration_sec").isNull() ) |
                ( col("session_duration_sec") < 0 ),
                lit("needs_review")
                    )
              .otherwise(lit("ready"))
             ) \
             .withColumn("session_duration_sec",
              when( (col("session_duration_sec").isNull()) | ( col("session_duration_sec") < 0 ), lit(fallback))
              .otherwise(col("session_duration_sec"))
             ) \
             .drop("data_volume_mb") \
             .withColumnRenamed("data_volume_mb_imputed", "data_volume_mb")
# Spark Session
spark = SparkSession.builder \
    .appName("KafkaCDRReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données JSON
cdr_schema = StructType() \
    .add("record_ID", StringType()) \
    .add("record_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("cell_id", StringType()) \
    .add("technology", StringType()) \
    .add("caller_id", StringType()) \
    .add("callee_id", StringType()) \
    .add("duration_sec", IntegerType()) \
    .add("sender_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("user_id", StringType()) \
    .add("data_volume_mb", DoubleType()) \
    .add("session_duration_sec", IntegerType())\
    .add("product_code", StringType())   

# Lire depuis Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "telecom") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
    
# Parser les valeurs JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), cdr_schema).alias("data")) \
    .select("data.*")

# Traitement robuste avec gestion des nulls
df_valid = df_parsed.filter(col("record_type").isNotNull()& col("timestamp").isNotNull())
df_valid = df_valid.filter(to_date(col("timestamp"), "yyyy-MM-dd") >= current_date())


def process_batch(df, epoch_id):
    # Compter les enregistrements
    from colorama import Fore, Style, init
    init()  # Initialize colorama (only needed on Windows)

    print(f"\n{Fore.GREEN}====================================={Style.RESET_ALL} {Fore.RED}{Style.BRIGHT}Batch {epoch_id}{Style.RESET_ALL} {Fore.GREEN}======================================={Style.RESET_ALL}")    
    
    total_count = df.count()
    print(f"{Fore.YELLOW} \n\nTotal des enregistrements:{Style.RESET_ALL} {total_count}")
    
    if total_count > 0:
        
    # Traitement par type d'enregistrement
        
        # Voice records
        print(f"{Fore.CYAN}\n\n=============================================================== Voice Records ===============================================================>{Style.RESET_ALL}")
        
        voice_df = df.filter(col("record_type") == "voice") \
                .drop("sender_id", "receiver_id", "user_id", "data_volume_mb", "session_duration_sec")
                
        voice_count = voice_df.count()
        print(f"\nVoice Records count: {voice_count}")
        
        if voice_count > 0:
            print("Voice Records (Before Analysis):")
            voice_df.show(truncate=False)
        
        voice_df= clean_voice_df(voice_df)
        
        voice_count = voice_df.count()
        print(f"Voice Records count: {voice_count}")
        if voice_count > 0:
            print("Voice Records (After Analysis):")
            voice_df.show(truncate=False)
            
            
        # SMS records
        print(f"{Fore.MAGENTA}<============================================================== SMS Records ==================================================================>{Style.RESET_ALL}")

        sms_df = df.filter(col("record_type") == "sms") \
                .drop("caller_id", "callee_id", "duration_sec", "user_id", "data_volume_mb", "session_duration_sec")
                
        sms_count = sms_df.count()
        print(f"\nSMS records count: {sms_count}")
        
        if sms_count > 0:
            print("SMS Records (Before Analysis):")
            sms_df.show(truncate=False)

        sms_df= clean_sms_df(sms_df)
        
        sms_count = sms_df.count()
        print(f"SMS records count: {sms_count}")
        
        if sms_count > 0:
            print("SMS Records (After Analysis):")
            sms_df.show(truncate=False)
            
            
        # Data records
        print(f"{Fore.YELLOW}<================================================================ Data Records =================================================================>{Style.RESET_ALL}")

        data_df = df.filter(col("record_type") == "data") \
                    .drop("sender_id", "receiver_id", "caller_id", "callee_id", "duration_sec")
        data_count = data_df.count()
        print(f"\nData Records count: {data_count}")
        if data_count > 0:
            print("Data Records (Before Analysis):")
            data_df.show(truncate=False)
        data_df= clean_data_df(data_df)
        
        data_count = data_df.count()
        print(f"Data Records count: {data_count}")
        
        if data_count > 0:
            print("Data Records (After Analysis):")
            data_df.show(truncate=False)
            
            
        # Concaténation des trois flux nettoyés
        cleaned_batch_df = voice_df \
            .unionByName(sms_df, allowMissingColumns=True) \
            .unionByName(data_df, allowMissingColumns=True) \
            .withColumn("batch_id", lit(epoch_id))  # Traçabilité

        # Écriture du batch nettoyé dans un dossier Parquet
        cleaned_batch_df.write.mode("append").partitionBy("record_type").parquet("mediated_data")

# Traitement personnalisé de chaque batch
query = df_valid.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()


from colorama import Fore, Style, init
init() 

try:
    # Format the waiting message with colors
    print(f"\n{Fore.CYAN}⏳ Spark streaming query is running...{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Press Ctrl+C to stop the processing{Style.RESET_ALL}")
    
    # Wait for termination
    query.awaitTermination()
    
except KeyboardInterrupt:
    # Format the shutdown message
    print(f"\n{Fore.RED}🛑 Received interrupt signal{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}⏳ Stopping Spark streaming query gracefully...{Style.RESET_ALL}")
    
    # Stop the query
    query.stop()
    
    # Confirmation message
    print(f"{Fore.GREEN}✅ Spark processing successfully stopped{Style.RESET_ALL}")
    
except Exception as e:
    # Handle other potential errors
    print(f"\n{Fore.RED}❌ Error occurred during Spark processing:{Style.RESET_ALL}")
    print(f"{Fore.RED}{str(e)}{Style.RESET_ALL}")
    query.stop()