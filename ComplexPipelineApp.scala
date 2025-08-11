import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ComplexPipelineApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ComplexPipelineApp")
      .master("local[*]")
      .getOrCreate()

    // Paso 1: Lectura de datos desde múltiples fuentes
    val csvData = readCSV(spark, "data/input.csv")
    val jsonData = readJSON(spark, "data/input.json")

    // Paso 2: Igualar esquemas de los DataFrames
    val csvWithSchema = alignSchema(csvData, List("Name", "Age", "City", "_corrupt_record"))
    val jsonWithSchema = alignSchema(jsonData, List("Name", "Age", "City", "_corrupt_record"))

    // Paso 3: Transformaciones en los datos
    val transformedCSV = transformData(csvWithSchema, "CSV")
    val transformedJSON = transformData(jsonWithSchema, "JSON")

    // Paso 4: Unión de datasets
    val unifiedDF = transformedCSV.union(transformedJSON)

    // Paso 5: Agregaciones
    val aggregatedDF = aggregateData(unifiedDF)

    // Paso 6: Escritura del resultado en Parquet
    writeParquet(aggregatedDF, "data/output.parquet")

    spark.stop()
  }

  // Función para leer datos CSV
  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }

  // Función para leer datos JSON
  def readJSON(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(path)
  }

  // Función para igualar el esquema de los DataFrames
  def alignSchema(df: DataFrame, requiredColumns: List[String]): DataFrame = {
    val existingColumns = df.columns.toSet
    val missingColumns = requiredColumns.filterNot(existingColumns.contains)

    // Agregar las columnas faltantes con valores nulos
    val dfWithMissingColumns = missingColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, lit(null).cast("string"))
    }

    // Seleccionar las columnas en el orden correcto
    dfWithMissingColumns.select(requiredColumns.map(col): _*)
  }

  // Función para transformar datos
  def transformData(df: DataFrame, source: String): DataFrame = {
    df.withColumn("Source", lit(source)) // Añadir columna para identificar la fuente
      .withColumn("ProcessedAt", current_timestamp()) // Añadir marca de tiempo
  }

  // Función para realizar agregaciones
  def aggregateData(df: DataFrame): DataFrame = {
    df.groupBy("Source")
      .agg(
        count("*").as("TotalRecords"),
        avg("Age").as("AverageAge"),
        max("Age").as("MaxAge"),
        min("Age").as("MinAge")
      )
  }

  // Función para escribir datos en Parquet
  def writeParquet(df: DataFrame, path: String): Unit = {
    df.write
      .mode("overwrite")
      .parquet(path)
  }
}
