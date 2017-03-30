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

    import spark.implicits._

    val productsDSRaw = spark.read.json(args(0)).
      withColumnRenamed(existingName = "announced-date", newName = "announced_date").as[Products]

    //converting Products DS to lowercase
    val productsDS = productsDSRaw.map(row => Products(row.announced_date,
                                      if(row.family != null) row.family.toLowerCase else null,
                                      row.manufacturer.toLowerCase,
                                      row.model.toLowerCase,
                                      row.product_name.toLowerCase)
                                      )

    val listingDSRaw = spark.read.json(args(1)).as[Listing]

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
      filter($"a.manufacturer".contains($"b.manufacturer") ||
        $"b.manufacturer".contains($"a.manufacturer") ||
        $"b.title".contains($"a.manufacturer")
      )
    // count: 1198704

    // remove words like for, fur
    // e.g. kameratasche modell hardcase, dunkelblau, im set mit 4 gb sd-card für sony dsc-w570, w530, w510, wx10, wx10, w310, 320, 350, 380, tx7, 10, t99, 110]

    val anotherFilter = filtered.filter(!$"b.title".contains(" für ") &&
      !($"b.title".contains(" for ")) &&
      !($"b.title".contains(" pour ")))
    // count: 1154491

    // split the model column on all delimiters like ' ', '-' etc
    // then see if either of them matches the title column
    val delimitedFilter = anotherFilter.withColumn("split_col", split($"model", "[\\-\\s]"))



  }


}