import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by krishna on 12/03/16.
  */
object TextProcessing {


  def tokenize(c: Column) = split(
    regexp_replace(lower(c), "[^a-zA-Z0-9\\s]", ""), "\\s+"
  )

  def textminig(df : DataFrame):DataFrame ={

    val myToken = udf((xs: String) => xs.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+"))
    val dftoken=  df.withColumn("token", myToken(df("title")))

    val stremover: StopWordsRemover = new StopWordsRemover()
      .setInputCol("token")
      .setOutputCol("processed_title")

    val htf:HashingTF = new HashingTF()
      .setInputCol(stremover.getOutputCol)
      .setOutputCol("rawFeatures")

    val nostop = stremover.transform(dftoken).drop("token")
    val tf = htf.transform(nostop)






    tf.select("processed_title","rawFeatures").show(3)
    return tf
  }



}
