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

val q = df.writeStream
            .foreachBatch { (batchDf: DataFrame, batchId: Long) => 

                // NOTE: the following commented code was one approach to ER that 
                // regenerates the "resolved entity" each time a new transaction
                // aligns to it
                //
                // val pkeys = batchDf.select("k").dropDuplicates().as[String].collect()
                // println(s"num pkeys: ${pkeys.length}")
                // val pKeyFilter = pkeys.map(x => s"k = '$x'").mkString(" OR ")
                // val df = spark
                //   .read
                //   .format("org.apache.spark.sql.cassandra")
                //   .options(Map( "table" -> "demo_table", "keyspace" -> "demo_ks"))
                //   .load()
                //   .filter(pKeyFilter)

                val df = batchDf.withColumn("elems", map(col("c"), col("v")))
                        .drop("c", "v")

                df.show(false)

                df.rdd.saveToCassandra("demo_ks", "demo_table_agg", SomeColumns("k", "elems" append))

                val after = spark
                  .read
                  .format("org.apache.spark.sql.cassandra")
                  .options(Map( "table" -> "demo_table_agg", "keyspace" -> "demo_ks"))
                  .load()

                after.show(false)

                println(s"batchId: $batchId")

                // .groupBy("value.id")
                // .agg(collect_list("value"))
            }
            .start()




