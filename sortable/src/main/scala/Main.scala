import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by rshah on 3/28/2017.
  */
object Main {

  case class Products (
                        announced_date: String,
                        family: String,
                        manufacturer: String,
                        model: String,
                        product_name: String
                      )

  case class Listing (
                      currency: String,
                      manufacturer: String,
                      price: String,
                      title: String
                     )


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder().
      master("local[*]").
      appName("sortable-test").
      getOrCreate()

    val productsDSRaw = spark.read.json("maprfs:///mapr/ri0.comscore.com/user/rshah/learning/products.txt").
      withColumnRenamed(existingName = "announced-date", newName = "announced_date").as[Products]

    //converting Products DS to lowercase
    val productsDS = productsDSRaw.map(row => Products(row.announced_date,
                                      if(row.family != null) row.family.toLowerCase else null,
                                      row.manufacturer.toLowerCase,
                                      row.model.toLowerCase,
                                      row.product_name.toLowerCase)
                                      )

    val listingDSRaw = spark.read.json("maprfs:///mapr/ri0.comscore.com/user/rshah/learning/listings.txt").as[Listing]

    // converting Listing DS to lowercase
    val listingDS = listingDSRaw.map(row => Listing(row.currency,
                                    row.manufacturer.toLowerCase,
                                    row.price,
                                    row.title.toLowerCase)
                                    ).distinct


    // getting a cross product of Products and Listings DS
    val cartesian = productsDS.as("a").join(listingDS.as("b"))

    // first layer of filtering where atleast manufacturer name should match
    // or manufacturer name of the product should occur in the listing title
    val filtered = cartesian.
      filter(col("a.manufacturer").contains(col("b.manufacturer")) ||
        col("b.manufacturer").contains(col("a.manufacturer")) ||
        col("b.title").contains(col("a.manufacturer"))
      )




  }


}
