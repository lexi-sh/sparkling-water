import org.apache.spark.sql.types.*

case class SchemaConverter(lineage: String = "", existingCaseClasses: Set[CaseClassString] = Set()):
  def generate(schema: StructType): Set[CaseClassString] =
    val fieldResponses = schema.fields.map(convertToScalaType)

    val fields = fieldResponses.mkString(sep, s",$sep", sep)
    val thisCaseClass = s"case class $lineage($fields)"
    val allNewCaseClasses = fieldResponses.flatMap(_.requiredCaseClasses).toSet
    allNewCaseClasses + thisCaseClass

  private def attachLineage(fieldName: String): String =
    if lineage.isEmpty then fieldName
    else s"$lineage${classNameFromColumnName(fieldName)}"

  private def convertToScalaType(
      field: StructField
  ): GeneratedType =
    val typeName = scalaTypeNameFromDataType(field.dataType, attachLineage(field.name))
    val withOption =
      if field.nullable then s"Option[$typeName]" else typeName

    field.dataType match
      case ArrayType(elementType, nullable) =>
      case MapType(keyType, valType, nullableValue) =>
      case t: StructType =>
        val caseClassReqs =
          val nestedGenerator = SchemaConverter(typeName, existingCaseClasses)
          val generatedCaseClassStrings = nestedGenerator.generate(t)
          existingCaseClasses ++ generatedCaseClassStrings

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
        val fieldNameWithLineage = attachLineage(fieldName)
        classNameFromColumnName(fieldName)
      case _ => "String"
