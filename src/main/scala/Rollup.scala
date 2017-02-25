/**
  * Created by oleg.baydakov on 18/02/2017.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

final case class PurchaseItem(
                               PurchaseID: Int,
                               Supplier: String,
                               PurchaseType: String,
                               PurchaseAmt: Double,
                               PurchaseDate: java.sql.Date
                             )

final case class Book(id: Int, author_ids: List[Int])


object Rollup {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Dataset Example").master("local[*]").getOrCreate()

    import spark.implicits._ // you must import implicits



    val orders = Seq(Book(1, List(1)), Book(2, List(1, 2)), Book(3, List(1, 2, 3))).toDS
    orders.createOrReplaceTempView("books")

    spark.sql("select * from books WHERE array_contains(author_ids, 1)").show

  }
}
