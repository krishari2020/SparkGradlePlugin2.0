package com.hari.learning.customfuncs.test

import org.apache.spark.sql.{ SparkSession, Row, Dataset, SaveMode }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object UsingSparkBuiltInFuncs {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val srcDf = spark.read.schema(StructType(new StructField("name", StringType, true)
      :: new StructField("age", IntegerType, false) :: new StructField("interaction_ts", TimestampType) :: Nil))
      .json("/tmp/mycustom_funcs/customer.json")
      .where(not(isnull('interaction_ts)))
      .where(hour('interaction_ts) >= 0 && hour('interaction_ts) < 12)
      .withColumn("offers", concat('name, lit(", thank you for visiting us . For your continued faith on us we would like to offer you a 10% discount coupon")))
    writeToTarget(srcDf, "/tmp/mybuilt_in_funcs/target/customer_jdbc_offers.json")
    Thread.sleep(20 * 1000)
  }

  def explain(df: Dataset[Row]): Unit = {
    df.explain
  }

  def writeToTarget(df: Dataset[Row], path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).json(path)
  }

}