package com.hari.learning.customfuncs.test

import org.apache.spark.sql.{ Dataset, SaveMode, SparkSession, Row }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.util.{ Calendar, Properties }

object UseMapPartitionWithJDBC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val jdbcProps = new Properties()
    jdbcProps.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver")
    jdbcProps.setProperty("user", "harim1")
    jdbcProps.setProperty("password", "harim1")
    val destStruct = StructType(new StructField("name1", StringType) :: new StructField("age1", IntegerType)
      :: new StructField("interaction_ts1", TimestampType) :: new StructField("greetings", StringType) :: Nil)
    val srcDf = spark.read.jdbc("jdbc:oracle:thin:@irl64ceq07:1521/orcl.informatica.com", "Customer", jdbcProps).where(not(isnull('interaction_ts))).mapPartitions(iter => {
      new Iterator[Row]() {
        override def hasNext: Boolean = iter.hasNext
        override def next: Row = {
          val inp = iter.next()
          val ts = inp.getTimestamp(2)
          val calend = Calendar.getInstance()
          calend.setTimeInMillis(ts.getTime)
          val isEvening = calend.get(Calendar.HOUR_OF_DAY) > 16 && calend.get(Calendar.HOUR_OF_DAY) < 20
          if (isEvening)
            new GenericRowWithSchema(Array[Any](inp.getString(0), inp.getInt(1), ts, s"Greetings ${inp.getString(0)}"), destStruct)
          else
            Row.empty
        }
      }
    })(RowEncoder(destStruct)).filter(r => r != Row.empty)
    // explain(srcDf)
    writeToTarget(srcDf, "/tmp/mycustom_funcs/target/greetings_customer.json")
    Thread.sleep(20000)
  }

  def writeToTarget(df: Dataset[Row], path: String): Unit = {
    //  df.write.format("console").option("truncate", "false").save()
    df.write.mode(SaveMode.Overwrite).json(path)
  }

  def explain(df: Dataset[Row]): Unit = {
    df.explain
  }

}