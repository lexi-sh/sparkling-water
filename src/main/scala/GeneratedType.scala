case class GeneratedType(name: String, typeName: String, requiredCaseClasses: Set[String] = Set()) {
  val annotations: Set[String] = Set()
  val annotationStr = if (annotations.nonEmpty) annotations.mkString(" ") + " " else ""
  override def toString: String = s"  $annotationStr$name: $typeName"
}