package common

object ExtensionMethods {
  implicit class RichBoolean(val b: Boolean) {
    def toYesNo = if (b) "Yes" else "No"
  }
}
