
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
      // .master("spark://10.136.126.189:7077")
      .master("local[2]")
      .appName("Spark Graph Frames")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val df = spark.read.json(Config.peopleJSONPath)
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()




