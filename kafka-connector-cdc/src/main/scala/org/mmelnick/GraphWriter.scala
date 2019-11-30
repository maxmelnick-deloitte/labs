package org.mmelnick

import com.datastax.spark.connector._
import org.apache.spark.sql.{Encoders, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class TransactionKey(id: String)

case class Entity(id: String, elem_id: String, source: String, properties: String)

case class TransactionValuePayload(entity: Entity)

object GraphWriter {
  def run(spark: SparkSession): Unit = {

    val keySchema = Encoders.product[TransactionKey].schema
    val schemaSchema = Encoders.product[ValueSchema].schema
    val payloadSchema = Encoders.product[TransactionValuePayload].schema

    val valSchema = new StructType().add("schema", schemaSchema)
      .add("payload", StringType)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transactions-topic")
      .option("startingOffsets", "earliest")
      .load()
      .select(
        from_json(col("key").cast("string"), keySchema) as "key",
        from_json(col("value").cast("string"), valSchema) as "value"
      )
      .withColumn("value", from_json(col("value.payload"), payloadSchema))
      .select(col("key.*"), col("value.*"))

    val q = df.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>

        val df = batchDf.withColumnRenamed("entity", "entities")
            .withColumn("entities", array("entities"))

        df.show(false)

        df.rdd.saveToCassandra("demo_ks", "resolved", SomeColumns("id", "entities" append))

        val after = spark
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "resolved", "keyspace" -> "demo_ks"))
          .load()

        after.show(false)

        println(s"batchId: $batchId")
      }
      .start()

    q.awaitTermination()
  }
}