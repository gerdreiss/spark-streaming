package common

import java.{ util => ju }

object ExtensionMethods {
  implicit class RichBoolean(val b: Boolean)     {
    def toYesNo: String = if (b) "Yes" else "No"
  }
  implicit class RichMap[K, V](val m: Map[K, V]) {
    def toJavaHashMap: ju.HashMap[K, V] =
      m.foldLeft(new ju.HashMap[K, V]()) { case (acc, (k, v)) =>
        acc.put(k, v)
        acc
      }
    def toJavaProperties: ju.Properties =
      m.foldLeft(new ju.Properties()) { case (acc, (k, v)) =>
        acc.put(k, v)
        acc
      }
  }
}
