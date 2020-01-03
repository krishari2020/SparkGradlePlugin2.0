package com.hari.gradle.spark.plugin.test.cluster

import org.apache.spark.sql.{ SparkSession, SaveMode }

/**
 * Move data from source to target in cluster mode.
 * @author harim
 *
 */

object SrcToTgt {

  def main(arsg: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    spark.read.json("/hari/source.json").write.mode(SaveMode.Overwrite).json("/hari/target.json")
  }
}