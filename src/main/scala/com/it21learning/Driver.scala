package com.it21learning

import com.it21learning.config.{ConsumerConfig, ProducerConfig, StreamingConsumerConfig}
import com.it21learning.streaming.model.Employee
import com.it21learning.streaming.{AvroConsumer, AvroProducer, StreamingAvroConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Driver {
  //the main entry
  def main(args: Array[String]): Unit = {
    //check
    if ( args != null && args.length > 1 && args(0) == "-t" ) {
      //action
      args(1) match {
        case "avroStreamingRead" => {
          //the spark-session
          val spark = SparkSession.builder.config(new SparkConf().set("spark.master", "local[3]")).getOrCreate()
          try {
            avroStreamingRead(spark)
          }
          finally {
            spark.stop()
          }
        }
        case "avroRead" => avroRead()
        case "avroWrite" => avroWrite()
        case _ => println("The action shouled only be one of avroRead, avroWrite, avroStreamingRead")
      }
    }
    else {
      //the spark-session
      val spark = SparkSession.builder.config(new SparkConf().set("spark.master", "local[*]")).getOrCreate()
      try {
        //simulate
        (new StockSimulator(spark)).run()
      }
      finally {
        spark.stop()
      }
    }
  }

  def avroWrite(): Unit = {
    //employees
    val employees = Array(
      Employee("John Smith", 21, "M", "Big Data Developer", "IT", "http://www.kgc.cn"),
      Employee("William Jungle", 32, "M", "Big Data Architect", "IT", "http://www.it21learning.com"),
      Employee("Michelle Kaggle", 27, "F", "Marketing Manager", "Sales", "http://www.kgc.cn")
    )

    //config发发发
    val cfg = ProducerConfig(Settings.brokerUrl, Settings.schemaRegistryUrl)
    import cfg.Settings
    //write
    (new AvroProducer("it21-streaming-producer") with Settings).produce[String, Employee](
      "it21-streaming",
      employees.map(e => (e.hashCode().toString, e)))
  }

  def avroRead(): Unit = {
    //config
    val cfg = ConsumerConfig(Settings.brokerUrl, Settings.schemaRegistryUrl)
    import cfg.Settings
    //read
    (new AvroConsumer("it21-streaming-consumer") with Settings).consume[String, Employee]("it21-streaming", print)
  }

  def avroStreamingRead(spark: SparkSession): Unit = {
    //config
    val cfg = StreamingConsumerConfig(Settings.brokerUrl, Settings.schemaRegistryUrl)
    import cfg.Settings
    //read
    (new StreamingAvroConsumer("it21-streaming-consumer") with Settings).start[String, Employee](spark, "it21-streaming", print)
  }

  def print(items: Seq[(String, Employee)]): Unit = items foreach { item =>
    println("The code is -> " + item._1)
    println("The employee is -> " + item._2.name)
  }
  def print(items: RDD[(String, Employee)]): Unit = print(items.collect())
}
