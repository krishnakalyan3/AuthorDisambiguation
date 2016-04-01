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
      .setMaster("local[4]")
      .setJars(List(jarloc))
    val sc = new SparkContext(conf)
    return sc
  }



  def main(args: Array[String]) {

    // Feature Matrix
    val feature_matrix:DataFrame = Features.features(sparkCont())
    //feature_matrix.show(10)
    val block1 = Statistics.stat(feature_matrix,"phonetic")

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
