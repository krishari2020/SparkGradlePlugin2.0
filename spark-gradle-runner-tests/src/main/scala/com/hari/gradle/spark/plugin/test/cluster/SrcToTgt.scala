package com.hari.gradle.spark.plugin.test.cluster

import org.apache.spark.sql.{ SparkSession, SaveMode }
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListener

/**
 * Move data from source to target in cluster mode.
 * @author harim
 *
 */

object SrcToTgt {

  def main(arsg: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    spark.read.json("/hari/source.json").write.mode(SaveMode.Overwrite).json("/hari/target.json")
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

        // The following section contains respective "metrics" object values.
        val inputMetrics = taskEnd.taskMetrics.inputMetrics
        val outputMetrics = taskEnd.taskMetrics.outputMetrics
        val shuffleReadMetrics = taskEnd.taskMetrics.shuffleReadMetrics
        val shuffleWriteMetrics = taskEnd.taskMetrics.shuffleWriteMetrics

        // The following section contains "metrics" primitive values
        val diskSpilled = taskEnd.taskMetrics.diskBytesSpilled
        val execCpuTime = taskEnd.taskMetrics.executorCpuTime
        val execDeCpuTime = taskEnd.taskMetrics.executorDeserializeCpuTime
        val execDeTime = taskEnd.taskMetrics.executorDeserializeTime
        val execRunTime = taskEnd.taskMetrics.executorRunTime
        val jvmGCTime = taskEnd.taskMetrics.jvmGCTime
        val memBytesSpilled = taskEnd.taskMetrics.memoryBytesSpilled
        val peakExecMem = taskEnd.taskMetrics.peakExecutionMemory
        val resSerTime = taskEnd.taskMetrics.resultSerializationTime
        val resultSize = taskEnd.taskMetrics.resultSize

        // Following section contains metrics for block related information.
        val blockInfo = taskEnd.taskMetrics.updatedBlockStatuses
      }
    })
  }

}