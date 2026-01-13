import org.apache.spark.sql.SparkSession

// Define the data model
case class Order(id: Int, customerId: String, amount: Double)

object FunctionalPipelineExample {

  def main(args: Array[String]): Unit = {

    // Stateful orchestration: Spark session + dataset
    val spark = SparkSession.builder()
      .appName("FunctionalPipelineExample")
      .master("local[*]") 
      .getOrCreate()

    import spark.implicits._

    val orders = Seq(
      Order(1, "A12", 50),
      Order(2, "B34", 200),
      Order(3, "C56", 175)
    )

    val ordersDF = spark.createDataset(orders)

    // Stateless transformations
    val isHighValue = (o: Order) => o.amount > 100
    val getCustomerId = (o: Order) => o.customerId

    // Pipeline: filter + map
    val highValueCustomerIds = ordersDF
      .filter(isHighValue)
      .map(getCustomerId)

    // Trigger and display results
    highValueCustomerIds.show()

    /*
      Output:
      +-----------+
      |value      |
      +-----------+
      |B34        |
      |C56        |
      +-----------+
    */

    spark.stop()
  }
}


