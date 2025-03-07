from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, mean
from pyspark.sql.types import StringType

# Inisialisasi sesi Spark
spark = SparkSession.builder.appName("TwitterUserAnalysis") .getOrCreate()

# Membaca data dari file Excel ke dalam DataFrame Spark
df = spark.read.format("csv") .option("header", True) .option("separator", True) .load("hdfs:///BigData/UAS/uas.csv")

# Kelas User
class User:
    def __init__(self, user_id, followers, following, last_post_tag):
        self.user_id = user_id
        self.followers = followers
        self.following = following
        self.last_post_tag = last_post_tag

# Fungsi untuk menghitung rata-rata following
def calculate_mean_following(df):
    mean_following = df.select(mean(col("friendsCount"))).first()[0]
    return mean_following

# Fungsi untuk mengklasifikasikan pengguna
def classify_user(followers, following, mean_following):
    followers = float(followers)
    following = float(following)
    if ((following >= mean_following and followers / following >= 11) or
        (following < mean_following and followers - following >= 10000)):
        return "influencer"
    else:
        return "biasa"

# Menghitung rata-rata following
mean_following = calculate_mean_following(df)

# Mendefinisikan UDF untuk mengklasifikasikan pengguna
classify_user_udf = udf(lambda followers, following: classify_user(followers, following, mean_following), StringType())

# Menambahkan kolom klasifikasi pengguna
df_with_classification = df.withColumn(
    "classification", 
    classify_user_udf(col("followersCount"), col("friendsCount"))
)

# Menampilkan DataFrame dengan klasifikasi
df_with_classification.show()

# Fungsi untuk menghitung jumlah tag unik
def count_unique_tags(tags_df):
    tags_rdd = tags_df.rdd.flatMap(lambda x: x)
    tag_counts = tags_rdd.map(lambda tag: (tag, 1)).reduceByKey(lambda a, b: a + b)
    return tag_counts

# WordCount untuk influencer dan biasa
classification_rdd = df_with_classification.select("classification").rdd.flatMap(lambda x: x)

# Menghitung jumlah influencer dan biasa menggunakan wordcount
classification_counts = classification_rdd.map(lambda classification: (classification, 1)).reduceByKey(lambda a, b: a + b)
print(classification_counts.collect())
