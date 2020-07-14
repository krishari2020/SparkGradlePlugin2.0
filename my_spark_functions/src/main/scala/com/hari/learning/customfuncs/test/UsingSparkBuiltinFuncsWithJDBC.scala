package com.hari.learning.customfuncs.test

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SparkSession, Row, Dataset, SaveMode }
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import java.util.Properties
import org.apache.spark.sql.functions._

object UsingSparkBuiltinFuncsWithJDBC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate
    import spark.implicits._
    val jdbcProps = new Properties()
    jdbcProps.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver")
    jdbcProps.setProperty("user", "harim1")
    jdbcProps.setProperty("password", "harim1")
    val srcDf = spark.read
      .jdbc("jdbc:oracle:thin:@irl64ceq07:1521/orcl.informatica.com", "customer", jdbcProps)
      .where(not(isnull('interaction_ts)))
      .where(hour('interaction_ts) >= 12 && hour('interaction_ts) < 16)
      .withColumn("offers", concat('name, lit(", thank you for visiting us . For your continued faith on us we would like to offer you a 10% discount coupon")))
    writeToTarget(srcDf, "/tmp/mycustom_funcs/target/customer_jdbc_offers.json")
    Thread.sleep(20 * 1000)
  }

  def writeToTarget(df: Dataset[Row], path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).json(path)
  }

  def explain(df: Dataset[Row]): Unit = {
    df.explain
  }

}