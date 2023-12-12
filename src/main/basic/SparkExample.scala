package main.basic

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Alice", 25),
      (2, "Bob", 30),
      (3, "Charlie", 22)
    )

    // Define the schema for the DataFrame
    val schema = List("id", "name", "age")

    // Create a DataFrame from the sample data and schema
    val df: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Perform a simple transformation (add 5 to the age column)
    val transformedDF = df.withColumn("age_plus_5", col("age") + 5)

    transformedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
