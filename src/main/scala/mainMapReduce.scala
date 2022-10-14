import HelperUtils.CreateLogger

object mainMapReduce {
  val logger = CreateLogger(classOf[mainMapReduce.type])
  @main def runmain(inputPath: String, outputPath: String): Unit =
    print("Input= "+ inputPath + " output="+ outputPath)
    logger.info("running main")
    MapReduce1.runMapReduce1(inputPath,outputPath)
    MapReduce2.runMapReduce2(inputPath,outputPath)
    MapReduce3.runMapReduce3(inputPath,outputPath)
    MapReduce4.runMapReduce4(inputPath,outputPath)
}
