package com.farooqkamal.freckle

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.slf4j.{Logger, LoggerFactory}

trait BaseSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with DatasetSuiteBase with Loggable {

  var sparkConf: SparkConf = _
  val logger = LoggerFactory.getLogger("location")

  override def conf: SparkConf = {
    sparkConf = super.conf
    sparkConf.set("spark.testing", "true")
  }

}
