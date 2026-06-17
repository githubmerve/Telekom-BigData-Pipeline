from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, regexp_replace, udf, current_timestamp, split, array_join
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import StopWordsRemover

# 1. Spark Oturumu - Konfigürasyonları hem genel hem spesifik olarak veriyoruz
spark = SparkSession.builder \
    .appName("Telekom_Nihai_Analiz_Sistemi") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Şema
schema = StructType([
    StructField("company", StringType(), True),
    StructField("date", StringType(), True),
    StructField("content", StringType(), True),
    StructField("link", StringType(), True)
])

# 3. Zenginleştirilmiş Duygu Analizi Motoru
def analiz_et(metin):
    # 🎯 KISITLAMA KALDIRILDI: Metin kısa da olsa analiz edilsin, akış kesilmesin
    if not metin: return "NOTR"
    puan = 0
    m = metin.lower()
    
    negatifler = ["cayma", "fahiş", "haksız", "icra", "borç", "pahalı", "zam", "yansıtılmış", 
                 "ücret", "aşım", "kesinti", "bedeli", "fatura", "soygun", "mağdur", "rezalet", 
                 "oyalıyor", "bekletildim", "iptal", "berbat", "pişman", "muhatap", "ilgisiz", 
                 "umursamaz", "çözülmedi", "erteleniyor", "nakil", "gecikme", "çekmiyor", 
                 "yavaş", "kopuyor", "sinyal", "arıza", "altyapı"]
    
    pozitifler = ["teşekkür", "memnun", "hızlı", "çözüldü", "uygun", "iyi", "başarılı", 
                 "harika", "güzel", "ilgili", "nazik", "kibar", "kaliteli", "çekiyor"]

    for kelime in negatifler:
        if kelime in m: puan -= 2
    for kelime in pozitifler:
        if kelime in m: puan += 2
    
    if "ama" in m or "fakat" in m or "istiyorum" in m:
        if -4 < puan < 4: return "NOTR (TALEP)"

    if puan <= -2: return "NEGATIF"
    elif puan >= 2: return "POZITIF"
    else: return "NOTR"

analiz_udf = udf(analiz_et, StringType())

# 4. Kafka'dan Oku
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sikayetler") \
    .option("startingOffsets", "earliest") \
    .load()

# 5. İşleme Katmanı (Metin Ön İşleme & Stop Words Temizliği)
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5.1. Küçük Harf ve Regex Temizliği
base_cleaned_df = json_df.withColumn("cleaned_content", lower(col("content"))) \
    .withColumn("cleaned_content", regexp_replace(col("cleaned_content"), r"[^a-zğüşıöç\s]", " ")) \
    .withColumn("cleaned_content", regexp_replace(col("cleaned_content"), r"\s+", " "))

# 5.2. Gelişmiş Türkçe Stop Words Listesi (Tez Analiz Standartlarına Göre)
turkce_stop_words = [
    "bir", "böyle", "kadar", "şu", "bu", "ve", "veya", "ile", "de", "da", "ki", "o", "bunu",
    "rağmen", "şekilde", "olarak", "tarafıma", "ancak", "yaklaşık", "en", "için", "ise", "mi",
    "ediyorum", "ederim", "büyük", "küçük", "gün", "saat", "ay", "yıl", "tane", "adet", "sonra",
    "turkcell", "vodafone", "turktelekom", "türk", "telekom", "şikayetvar", "merhaba"
]

# 5.3. Metni Kelimelere Ayırma (StopWordsRemover dizi formatı bekler)
words_df = base_cleaned_df.withColumn("words_array", split(col("cleaned_content"), " "))

# 5.4. Stop Words Kaldırma İşlemi
remover = StopWordsRemover(inputCol="words_array", outputCol="filtered_words", stopWords=turkce_stop_words)
filtered_df = remover.transform(words_df)

# 5.5. Kelimeleri Tekrar Cümle Yapma ve Duygu Analizini Tetikleme
processed_df = filtered_df.withColumn("cleaned_content", array_join(col("filtered_words"), " ")) \
    .withColumn("duygu", analiz_udf(col("cleaned_content"))) \
    .withColumn("islem_zamani", current_timestamp())

# 🎯 İMHA EDİLDİ: dropDuplicates kalkanı tamamen kaldırıldı! 
# Artık hiçbir veri RAM üzerinde havada elenmeyecek, Kafka'dan ne gelirse geçecek.
# processed_df = base_processed_df.dropDuplicates(["cleaned_content"])

# 6. MongoDB Yazma Fonksiyonu
def write_to_mongo(batch_df, batch_id):
    try:
        batch_df.write.format("mongodb") \
            .mode("append") \
            .option("database", "TelekomAnaliz") \
            .option("collection", "Yorumlar") \
            .option("writeConcern.w", "1") \
            .option("ordered", "false") \
            .save()
    except Exception as e:
        if "E11000" in str(e) or "duplicate key error" in str(e) or "BulkWriteException" in str(e):
            print(f"-> Batch {batch_id}: Kopyalar akıştan başarıyla elendi, yeni veriler veritabanına yazıldı.")
        else:
            print(f"-> Batch {batch_id} üzerinde beklenmeyen hata: {str(e)}")

# 7. Çıktılar (Console ve MongoDB Stream Tetikleme)
query_console = processed_df.select("company", "duygu", "cleaned_content") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

query_mongo = processed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "/tmp/spark_mongo_final_res") \
    .start()

spark.streams.awaitAnyTermination()

