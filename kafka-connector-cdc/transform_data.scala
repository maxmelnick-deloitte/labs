// ./bin/spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.2,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

val keySchema = new StructType().add("k", StringType).add("c", StringType)

val schemaSchema = new StructType().add("type", StringType).add("optional", BooleanType)
val payloadSchema = new StructType().add("v", StringType)

val valSchema = new StructType().add("schema", schemaSchema)
                    .add("payload", StringType)

val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "demo-topic")
  .option("startingOffsets", "earliest")
  .load()
  .select(
        from_json(col("key").cast("string"), keySchema) as "key",
        from_json(col("value").cast("string"), valSchema) as "value"
    )
  .withColumn("value", from_json(col("value.payload"), payloadSchema))
  .select(col("key.*"), col("value.*"))

// df.show(false)
  // .select()
  // .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  // .as[(String, String)]

val q = df.writeStream
            // .outputMode("update")
            // .format("console")
            // .option("truncate", "false")
            .foreachBatch { (batchDf: DataFrame, batchId: Long) => 
                val existing = spark
                  .read
                  .format("org.apache.spark.sql.cassandra")
                  .options(Map( "table" -> "demo_table", "keyspace" -> "demo_ks"))
                  .load()
                batchDf.show(false)
                println(s"batchId: $batchId")
            }
            .start()




