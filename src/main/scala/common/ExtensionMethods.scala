package common

object ExtensionMethods {
  implicit class RichBoolean(val b: Boolean)     {
    def toYesNo: String = if (b) "Yes" else "No"
  }
  implicit class RichMap[K, V](val m: Map[K, V]) {
    def toJavaHashmap: java.util.HashMap[K, V] =
      m.foldLeft(new java.util.HashMap[K, V]()) { case (acc, (k, v)) => acc.put(k, v); acc }
  }
}
