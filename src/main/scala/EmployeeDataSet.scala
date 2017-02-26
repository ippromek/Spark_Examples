import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**
  * Created by ganeshchand on 12/20/16.
  */

case class Employee(id: Long, dept_id: Long, name: String, age: Int, email: String,sex: String)

object EmployeeDataSet {
  def main(args: Array[String]): Unit = {


    // Check whether input arguments are present
   /* if (args.length < 2) {
      System.err.println("Usage: \n" +
        "./bin/spark-submit --class org.abogdanov.spark_scala_examples.KMeansClustering " +
        "./<jarName> <inputFile> <outputDir>")
      System.exit(1)
    } */


    val spark = SparkSession.builder().appName("Dataset Example").master("local[*]").getOrCreate()

    // data source path
    val csvFilePath = "src/main/resources/employee.csv"


    val jsonFilePath = "src/main/resources/employee.json"
    val parquetFilePath = "src/main/resources/employee.parquet"



    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvFilePath)

    val differentCallTypes = df.count()
    println("Different number of Call Types " + differentCallTypes)



    // define a schema - approach 1
    val id         = StructField("id",       DataTypes.LongType)
    val dept_id    = StructField("dept_id",    DataTypes.LongType)
    val name       = StructField("name",   DataTypes.StringType)
    val age        = StructField("age", DataTypes.IntegerType)
    val email      = StructField("email",    DataTypes.StringType)
    val sex        = StructField("sex",    DataTypes.StringType)

    val fields = Array(id, dept_id, name, age, email, sex)
    val employeeSchema1 = StructType(fields)


    // define a schema - approach 2
    val employeeSchema2 = new StructType()
      .add("id", LongType, false)
      .add("dept_id", LongType, false)
      .add("name", StringType, false)
      .add("age", IntegerType, false)
      .add("email", StringType, false)
      .add("sex", StringType, false)

    import spark.implicits._ // you must import implicits

    //     reading data source file
    val employee = spark.read.schema(employeeSchema1).option("header", true).csv(csvFilePath).as[Employee]

    // val employee = spark.read.schema(employeeSchema2).json(jsonFilePath).as[Employee] // for JSON file

    val maleEmp45Above = employee.dropDuplicates().filter(emp => emp.age >= 45 && emp.sex == "M")

    // write

    maleEmp45Above.write.mode("overwrite").saveAsTable("address")
    //val address = spark.read.table("address")

    spark.catalog.listTables.show
    spark.sql("select count(*) from address").show
    //address.show


    spark.read.schema(employeeSchema1).option("header", true).csv(csvFilePath).withColumn("input_file_name", input_file_name()).show(5)

    employee.createOrReplaceTempView("employee")
    spark.sql("select spark_partition_id() from employee").toDF().show(5)
    spark.catalog.dropTempView("tmp_table")


    // store the catalog in a shorter variable (less typing)
    //val cat = spark.catalog

    // show current database
    //cat.listDatabases.show(false)
    //cat.listTables("default").show

    //cat.listColumns("address").show

   // maleEmp45Above.write.mode("overwrite").parquet("/tmp/spark/output/parquet/maleEmp45Above")
   // maleEmp45Above.coalesce(1).write.option("header",true).mode("overwrite").csv("/tmp/spark/output/csv/maleEmp45Above")

  }
}