import org.apache.spark.sql.SparkSession

object SimpleApp_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SimpleApp_2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(("Alice", 25), ("Bob", 30), ("Cathy", 28))
    val df = data.toDF("Name", "Age")

    df.show()

    spark.stop()
  }
}
