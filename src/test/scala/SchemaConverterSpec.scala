import org.scalatest.*
import flatspec.*
import matchers.*
import org.apache.spark.sql.types.*

class SchemaConverterSpec extends AnyFlatSpec with should.Matchers:

  "simple schema" should "be converted to single case class" in {

    val schema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("age", IntegerType, false),
        StructField("address", StringType, false)
      )
    )
    val actual =
      combine(SchemaConverter().generate("MyRoot", schema), "my.package")
    val expected =
      """package my.package

case class MyRoot(
  name: Option[String],
  age: Int,
  address: String
)"""
    actual should be(expected)
  }

  
  
  "complex schema" should "be converted correctly" in {
    import org.apache.spark.sql.types.*
    import org.apache.spark.sql.functions.*

// Define a complex schema with various nested types
    val complexSchema = StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("binaryData", BinaryType, true),
        StructField("mapField", MapType(StringType, StringType), true),
        StructField(
          "mapWithStructValue",
          MapType(
            StringType,
            StructType(
              Array(
                StructField("nestedField1", StringType, true),
                StructField("nestedField2", IntegerType, true)
              )
            )
          ),
          true
        ),
        StructField("timestampField", TimestampType, true),
        StructField("dateField", DateType, true),
        StructField(
          "complexNestedStruct",
          StructType(
            Array(
              StructField(
                "level1",
                StructType(
                  Array(
                    StructField(
                      "level2",
                      StructType(
                        Array(
                          StructField("sameFieldName", StringType, true),
                          StructField("anotherField", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("sameFieldName", StringType, true)
                  )
                ),
                true
              ),
              StructField(
                "arrayOfStructs",
                ArrayType(
                  StructType(
                    Array(
                      StructField("structInArrayField1", LongType, true),
                      StructField("structInArrayField2", BooleanType, true)
                    )
                  ),
                  true
                ),
                true
              )
            )
          ),
          true
        ),
        StructField(
          "mapOfStructs",
          MapType(
            StringType,
            StructType(
              Array(
                StructField("innerStructField1", DecimalType(10, 2), true),
                StructField(
                  "innerStructField2",
                  ArrayType(StringType, true),
                  true
                )
              )
            )
          ),
          true
        )
      )
    )

    val actual =
      combine(SchemaConverter().generate("MyRoot", complexSchema), "my.package")
    println(actual)
    // actual should contain mapWithStructValue
    actual should include("mapWithStructValue: Map[String, MyRootMapWithStructValue]")
    actual should include("case class MyRootMapWithStructValue(")
    actual should include("nestedField1: String")
    actual should include("nestedField2: Int")
    actual should include("case class MyRootComplexNestedStruct(")
    actual should include("level1: MyRootLevel1")
    actual should include("case class MyRootLevel")
  }
