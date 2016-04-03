import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by krishna on 12/03/16.
  */
object ADMain extends AnyRef{



  def sparkCont(): SparkContext ={
    val jarloc = "/Users/krishna/IdeaProjects/AD/target/scala-2.10/ad_2.10-1.0.jar"
    val conf = new SparkConf().setAppName("AD")
      .setSparkHome("/Users/krishna/spark")
      .setMaster("spark://Krishna:7077")
      .setJars(List(jarloc))
    val sc = new SparkContext(conf)
    return sc
  }



  def main(args: Array[String]) {

    // My Spark Context
    val sc = sparkCont()

    // Feature Matrix
    val feature_matrix:DataFrame = Features.features(sc)

    // filtered Block
    //val block : DataFrame = DataBlock.block(feature_matrix)
    //block.show(30)

    //writetoCSV(block,sc, "/Users/krishna/Dropbox/DMKM/Course/SEM2/DataPreprocessing/block/disb")

    //val numerical :DataFrame = Numericalization.numericalMatrix(block)


    //block.show(10)
    //val op1 =block.filter("authorFullGT is not null")
    //op1.show(10)
    // val op=  op1.count()
    //print(op + "   ### Dis- Count ###")



    //val block : DataFrame = ReadFromCSV.readFreomCSV(sc,
    //      "/Users/krishna/Dropbox/DMKM/Course/SEM2/DataPreprocessing/block/output","true")

    //block.show(88)
    //print("Total Authors " + block.count())


    //val numerical_matrix : DataFrame = numericalMatrix(feature_matrix)

    //val groundtruth = DataLoad_GT.data(sparkCont()).show(3)


    //feature_matrix.show(10)
    //val block1 = Statistics.stat(feature_matrix,"phonetic")

    //val distanceModelling: DataFrame = DistanceModelling.dml(feature_matrix)


    //WriteToCSV.writetoCSV(feature_matrix,sc,"/Users/krishna/MOOC/Pre-Process/predata")


    // StopWord removal , tokenization
    //val text_prcessing: DataFrame = TextProcessing.textminig(feature_matrix)
    //text_prcessing.select("processed_title").show(3)
  /*
    val sqlContext = new SQLContext(sparkCont())
    val featuers = sqlContext.load(
      "com.databricks.spark.csv",
      Map("path" -> "/Users/krishna/MOOC/Pre-Process/predata.csv", "header" -> "true", "inferSchema" -> "true"))
    //featuers.printSchema()

*/
  }

}
