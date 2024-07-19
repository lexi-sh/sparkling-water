def classNameFromColumnName(columnName: String): String =
  val parts = columnName.split("_")
  val name = parts.map(_.capitalize).mkString
  s"Gen$name"

val sep: String = sys.props("line.separator")

def combine(s: Set[CaseClassString], pkg: String): String =
    val classes = s.mkString(sep)
    s"package $pkg$sep$sep$classes"

type CaseClassString = String
