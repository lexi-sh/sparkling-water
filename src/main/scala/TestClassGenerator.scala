import org.apache.spark.sql.SparkSession

case class GeneratedCaseClass(topLevelClassName: String, dataString: String, caseClassCodeString: String)

object TestClassGenerator {
  def generate(spark: SparkSession, sparkQuery: String, topLevelClassName: String, limit: Int = 500, packageName: String): GeneratedCaseClass = {
    // query
    // get dataframe
    // get schema
    // something should generate the class strings
    ???
  }
}
