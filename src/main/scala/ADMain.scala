import org.apache.spark.sql.DataFrame

/**
  * Created by krishna on 12/03/16.
  */
object ADMain {

  def main(args: Array[String]) {

    // 4 - number of threads to be used
    val feature_matrix:DataFrame = Features.features(4)

    // StopWord removal , tokenization
    //val text_prcessing: DataFrame = TextProcessing.textminig(feature_matrix)
    //text_prcessing.select("processed_title").show(3)


  }

}
