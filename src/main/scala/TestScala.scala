import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object TestScala {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\hadoop-winutils-2.6.0\\")



    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession.builder
     // .master("spark://10.136.126.189:7077")
      .master("local[2]")
      .appName("Spark Graph Frames")
   //   .config("spark.sql.warehouse.dir", warehouseLocation)
   //   .enableHiveSupport()
      .getOrCreate()

    // ------------------- SPARK SESSION ---------------
    //set new runtime options
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")
    //get all settings
    val configMap:Map[String, String] = spark.conf.getAll
    println(configMap.mkString("\n"))

    val configMap1:scala.collection.Map[String, (Long, Long)]=spark.sparkContext.getExecutorMemoryStatus
    println(configMap1.mkString("\n"))

//    println(spark.sparkContext.getConf.toDebugString)

    //fetch metadata data from the catalog
    spark.catalog.listDatabases.show(false)
    spark.catalog.listTables.show(false)


    //----------------------------------------------------
    LogManager.getRootLogger.setLevel(Level.ERROR)

  /*  import spark.sqlContext.implicits._

    val df = spark.read.json(Config.peopleJSONPath)
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show() */


  //  val lines = spark.sparkContext.parallelize(Seq("Spark Intellij Idea Scala test one", "Spark Intellij Idea Scala test two", "Spark Intellij Idea Scala test three"))

  //  val counts = lines.flatMap(line => line.split(" "))
  //    .map(word => (word, 1))
  //    .reduceByKey(_ + _)

  //  counts.saveAsTextFile("C:\\1\\1\\count.txt")


  //  counts.foreach(println)
  }
}
