package helper

object ArgsHelper {
  def argsHelper(args: Array[String]):scala.collection.mutable.Map[String, String] = {

    var parameters = scala.collection.mutable.Map[String, String]()

    if (args.length == 0) {
      return parameters
    }

    val firstArg = args(0)

    parameters += ("Master" -> firstArg)

    if (firstArg.equals("help")) {
      showHelpCommand()
    }

    parameters
  }

  def showHelpCommand() = {
    println("Proper Usage is xxx")
  }


}
