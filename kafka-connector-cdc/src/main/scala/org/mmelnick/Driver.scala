package org.mmelnick

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName("Kafka CDC example")
      .enableHiveSupport()
      .getOrCreate()

    // by default this loads the config from src/main/resources/application.conf
    val conf: Config = ConfigFactory.load()

    AggTransactions.run(spark, conf)

    spark.stop()
    sys.exit(0)
  }
}