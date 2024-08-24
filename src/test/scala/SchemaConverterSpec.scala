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
      combine(SchemaConverter("MyRoot").generate(schema), "my.pkg")
    val expected =
      """package my.pkg

case class MyRoot(
  name: Option[String],
  age: Int,
  address: String
)"""
    actual should be(expected)
  }

  "schema with map of struct" should "have struct class included in case class list" in {

    val schema = StructType(
      Array(
        StructField("age", IntegerType, false),
        StructField("person", StructType(
          Array(
            StructField("name", StringType, true),
            StructField("address", StringType, true)
          )
        ), false)
      )
    )
    val actual =
      combine(SchemaConverter("PersonRoot").generate(schema), "my.pkg")
    val expected =
      """package my.pkg

case class PersonRootPerson(
  name: Option[String],
  address: Option[String]
)
case class PersonRoot(
  age: Int,
  person: PersonRootPerson
)"""
    actual should be(expected)
  }


  "schema with using the same struct type in multiple fields" should "NOT reuse the same struct" in {

    val personStructType = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("address", StringType, true)
      )
    )
    val schema = StructType(
      Array(
        StructField("age", IntegerType, false),
        StructField("person1", personStructType, false),
        StructField("person2", personStructType, false),
      )
    )
    val actual =
      combine(SchemaConverter("PersonRoot").generate(schema), "my.pkg")
    val expected =
      """package my.pkg

case class PersonRootPerson1(
  name: Option[String],
  address: Option[String]
)
case class PersonRootPerson2(
  name: Option[String],
  address: Option[String]
)
case class PersonRoot(
  age: Int,
  person1: PersonRootPerson1,
  person2: PersonRootPerson2
)"""
    actual should be(expected)
  }


  "schema with map of struct with another substruct" should "have both struct classes included in case class list" in {

    val schema = StructType(
      Array(
        StructField("age", IntegerType, false),
        StructField("person", StructType(
          Array(
            StructField("name", StringType, true),
            StructField("address", StructType(
              Array(
                StructField("line1", StringType, false),
                StructField("line2", StringType, true)
              )
            ), true)
          )
        ), false)
      )
    )
    val actual =
      combine(SchemaConverter("Person").generate(schema), "my.pkg")
    val expected =
      """package my.pkg

case class PersonPersonAddress(
  line1: String,
  line2: Option[String]
)
case class PersonPerson(
  name: Option[String],
  address: Option[PersonPersonAddress]
)
case class Person(
  age: Int,
  person: PersonPerson
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
        StructField(
          "requiredMapWithStructValue",
          MapType(
            StringType,
            StructType(
              Array(
                StructField("nestedField1", StringType, true),
                StructField("nestedField2", IntegerType, true)
              )
            )
          ),
          false
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
      combine(SchemaConverter("MyRoot").generate(complexSchema), "my.pkg")
    println(actual)
    // actual should contain mapWithStructValue
    actual should include("mapWithStructValue: Option[scala.collection.Map[String, MyRootMapWithStructValue]]")
    actual should include("requiredMapWithStructValue: scala.collection.Map[String, MyRootRequiredMapWithStructValue]")
    actual should include("case class MyRootMapWithStructValue(")
    actual should include("nestedField1: String")
    actual should include("nestedField2: Int")
    actual should include("case class MyRootComplexNestedStruct(")
    actual should include("level1: MyRootLevel1")
    actual should include("case class MyRootLevel")
  }

