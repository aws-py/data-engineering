from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

# Crear sesión de PySpark
spark = SparkSession.builder.master("local[*]").appName("UnstructuredData").getOrCreate()

# Datos simulados en formato JSON
data = [(101, '{"details": {"name": "Alice", "age": 30, "preferences": {"theme": "dark"}}}')]
df = spark.createDataFrame(data, ["user_id", "json_data"])

# Extraer datos JSON
df_extracted = df.withColumn("name", get_json_object(col("json_data"), "$.details.name")) \
                 .withColumn("age", get_json_object(col("json_data"), "$.details.age")) \
                 .withColumn("theme", get_json_object(col("json_data"), "$.details.preferences.theme"))

df_extracted.show()


