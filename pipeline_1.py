from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Criar SparkSession
spark = SparkSession.builder \
    .appName("OptimizedPipeline") \
    .config("spark.sql.broadcastTimeout", "1200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# Configuração de logging para depuração
spark.sparkContext.setLogLevel("INFO")

# 1. Leitura dos dados em formato Parquet
sales_df = spark.read.format("parquet").load("s3://path-to-bronze-layer/sales/")
customers_df = spark.read.format("parquet").load("s3://path-to-bronze-layer/customers/")
products_df = spark.read.format("parquet").load("s3://path-to-bronze-layer/products/")

# Mostrar os esquemas das tabelas (opcional para debugging)
sales_df.printSchema()
customers_df.printSchema()
products_df.printSchema()

# 2. Otimização: Cache para tabelas pequenas
customers_df = customers_df.persist()
products_df = spark.sql("CACHE TABLE products")

# 3. Uso de broadcasting para joins com tabelas pequenas
enriched_sales_df = sales_df \
    .join(F.broadcast(customers_df), on="customer_id", how="inner") \
    .join(F.broadcast(products_df), on="product_id", how="inner")

# 4. Transformações adicionais
aggregated_df = enriched_sales_df.groupBy("customer_id", "product_id") \
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("sales_amount").alias("total_sales"),
        F.avg("sales_amount").alias("average_sales")
    )

# 5. Persistência: Manter dados processados em memória e disco (opcional)
aggregated_df = aggregated_df.persist()

# 6. Otimização de particionamento para gravação
output_path = "s3://path-to-silver-layer/aggregated_sales/"
aggregated_df \
    .repartition(10, F.col("customer_id"))  # Reparticionar para otimizar consultas futuras
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .save(output_path)

# Liberar recursos de persistência
aggregated_df.unpersist()
customers_df.unpersist()

# Finalizar SparkSession
spark.stop()
