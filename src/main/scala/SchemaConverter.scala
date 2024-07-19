import org.apache.spark.sql.types.*

case class SchemaConverter(lineage: String = "", existingCaseClasses: Set[CaseClassString] = Set()):
  def generate(name: String, schema: StructType): Set[CaseClassString] =
    val fieldResponses = schema.fields.map(convertToScalaType)

    val fields = fieldResponses.mkString(sep, s",$sep", sep)
    val thisCaseClass = s"case class $name($fields)"
    val allNewCaseClasses = fieldResponses.flatMap(_.requiredCaseClasses).toSet
    allNewCaseClasses + thisCaseClass

  private def convertToScalaType(
      field: StructField
  ): GeneratedType =
    val fieldName = scalaTypeNameFromDataType(field.dataType, field.name)
    val withOption =
      if field.nullable then s"Option[$fieldName]" else fieldName

    field.dataType match
      case t: StructType =>
        val caseClassReqs = if !existingCaseClasses.contains(fieldName) then
          val nestedGenerator = SchemaConverter(fieldName, existingCaseClasses)
          val generatedCaseClassStrings = nestedGenerator.generate(fieldName, t)
          existingCaseClasses ++ generatedCaseClassStrings
        else existingCaseClasses
        GeneratedType(field.name, withOption, caseClassReqs)

      case _             => GeneratedType(field.name, withOption, existingCaseClasses)

  private def scalaTypeNameFromDataType(
      dType: DataType,
      fieldName: String
  ): String =
    dType match
      case ByteType      => "Byte"
      case ShortType     => "Short"
      case IntegerType   => "Int"
      case LongType      => "Long"
      case FloatType     => "Float"
      case DoubleType    => "Double"
      case StringType    => "String"
      case BinaryType    => "Array[Byte]"
      case BooleanType   => "Boolean"
      case TimestampType => "java.sql.Timestamp"
      case DateType      => "java.sql.Date"
      case ArrayType(elementType, _) =>
        s"Seq[${scalaTypeNameFromDataType(elementType, fieldName)}]"
      case MapType(keyType, valType, _) =>
        val keyTypeStr = scalaTypeNameFromDataType(keyType, fieldName)
        val valTypeStr = scalaTypeNameFromDataType(valType, fieldName)
        s"scala.collection.Map[$keyTypeStr, $valTypeStr]"
      case t: StructType =>
        val fieldNameWithLineage =
          s"$lineage$$${classNameFromColumnName(fieldName)}"
        classNameFromColumnName(fieldName)
      case _ => "String"
