package com.merge.helper

object ArgsHelper {
  def argsHelper(args: Array[String]):scala.collection.mutable.Map[String, String] = {

    var parameters = scala.collection.mutable.Map[String, String]()
    if (args.length != 2) {
      showHelpCommand()
    }

    val master = args(0)
    val path = args(1)

    parameters += ("Master" -> master)
    parameters += ("Path" -> path)

    parameters
  }

  def showHelpCommand() = {
    println("Two arguments are needed, first one is sparkUrl and second one is path to this project")
  }


}
