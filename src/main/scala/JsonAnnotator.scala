
import org.apache.spark.sql.types.*

trait SerializationProcess {
  val extraImports: Seq[String]
  val annotationWriter: Map[DataType, String]

}

object JacksonJsonAnnotator extends SerializationProcess {
  override val extraImports: Seq[String] = List("com.fasterxml.jackson.annotation.JsonInclude", "com.fasterxml.jackson.annotation.JsonProperty")
  override val annotationWriter: Map[DataType, String] = Map(
    LongType -> "@JsonDeserialize(contentAs = classOf[java.lang.Long])",
    FloatType -> "@JsonDeserialize(contentAs = classOf[Float])",
  )
}
