import java.io.PrintWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object WordCount {
  def main(args: Array[String]): Unit = {
    // Crear el archivo de texto
    val writer = new PrintWriter("ruta/al/archivo.txt")
    writer.println("Este es un ejemplo de texto para contar palabras.")
    writer.close()

    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]") // Ejecutar en modo local
      .getOrCreate()

    import spark.implicits._ // Importar implicits para encoders

    // Leer el archivo de texto
    val textFile = spark.read.textFile("ruta/al/archivo.txt")

    // Contar las palabras
    val wordCounts = textFile
      .flatMap(line => line.split(" ")) // Dividir cada línea en palabras
      .groupByKey(identity) // Agrupar por palabra
      .count() // Contar las ocurrencias

    // Mostrar los resultados
    wordCounts.show()

    spark.stop()
  }
}
