package com.hari.gradle.spark.plugin.test.local

import org.apache.spark.sql.{ SparkSession, SaveMode }

/**
 * Move simple data from source to target in local mode.
 * @author harim
 *
 */

object SrcToTgt {

  def main(arsg: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    spark.read.json("src/main/resources/source.json").write.mode(SaveMode.Overwrite).json("src/main/resources/target.json")
  }
}