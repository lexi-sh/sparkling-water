object SparkClient:
  import org.apache.spark.sql.SparkSession
  def get(connectionString: String): SparkSession =
    SparkSession.builder().remote(connectionString).getOrCreate()
