from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Crear una SparkSession
spark = (SparkSession.builder
         .appName("MiAppPySpark")
         .getOrCreate()
         )

# Crear un DataFrame con datos de ejemplo
data = [
    (1, 101, "recibido"), (1, 101, "inicio"), (1, 101, "medio"), (1, 101, "final"), (1, 101, "enviado"),
    (2, 102, "recibido"), (2, 102, "inicio"), (2, 102, "medio"), (2, 102, "final"), (2, 102, "rechazado"),
    (3, 103, "recibido"), (3, 103, "inicio"), (3, 103, "medio"), (3, 103, "final"), (3, 103, "perdido"),
    (4, 104, "recibido"), (4, 104, "inicio"), (4, 104, "medio"), (4, 104, "final"), (4, 104, "enviado"),
    (5, 105, "recibido"), (5, 105, "inicio"), (5, 105, "medio"), (5, 105, "final"), (5, 105, "rechazado"),
    (6, 106, "recibido"), (6, 106, "inicio"), (6, 106, "medio"), (6, 106, "final"), (6, 106, "enviado"),
    (7, 107, "recibido"), (7, 107, "inicio"), (7, 107, "medio"), (7, 107, "final"), (7, 107, "perdido"),
    (8, 108, "recibido"), (8, 108, "inicio"), (8, 108, "medio"), (8, 108, "final"), (8, 108, "enviado"),
    (9, 109, "recibido"), (9, 109, "inicio"), (9, 109, "medio"), (9, 109, "final"), (9, 109, "rechazado"),
    (10, 110, "recibido"), (10, 110, "inicio"), (10, 110, "medio"), (10, 110, "final"), (10, 110, "enviado"),
    (11, 111, "recibido"), (11, 111, "inicio"), (11, 111, "medio"), (11, 111, "final"), (11, 111, "enviado"),
    (12, 112, "recibido"), (12, 112, "inicio"), (12, 112, "medio"), (12, 112, "final"), (12, 112, "rechazado"),
    (13, 113, "recibido"), (13, 113, "inicio"), (13, 113, "medio"), (13, 113, "final"), (13, 113, "perdido"),
    (14, 114, "recibido"), (14, 114, "inicio"), (14, 114, "medio"), (14, 114, "final"), (14, 114, "enviado"),
    (15, 115, "recibido"), (15, 115, "inicio"), (15, 115, "medio"), (15, 115, "final"), (15, 115, "enviado"),
    (16, 116, "recibido"), (16, 116, "inicio"), (16, 116, "medio"), (16, 116, "final"), (16, 116, "rechazado"),
    (17, 117, "recibido"), (17, 117, "inicio"), (17, 117, "medio"), (17, 117, "final"), (17, 117, "perdido"),
    (18, 118, "recibido"), (18, 118, "inicio"), (18, 118, "medio"), (18, 118, "final"), (18, 118, "enviado"),
    (19, 119, "recibido"), (19, 119, "inicio"), (19, 119, "medio"), (19, 119, "final"), (19, 119, "rechazado"),
    (20, 120, "recibido"), (20, 120, "inicio"), (20, 120, "medio"), (20, 120, "final"), (20, 120, "enviado")
]
columnas = ["id", "id_proyecto", "status_id"]
df = spark.createDataFrame(data, columnas)

# Definir una ventana para la función lag
window_spec = Window.partitionBy("id_proyecto").orderBy("id")

# Añadir una columna con el siguiente estado
df_transformed = df.withColumn("siguiente_status", F.lead("status_id", 1).over(window_spec))

# Mostrar el resultado
df_transformed.show()

# Guardar el resultado en formato Parquet
#df_transformed.write.mode("overwrite").parquet("ruta/de/salida")

# Finalizar la SparkSession
spark.stop()
