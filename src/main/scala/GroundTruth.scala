import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
/**
  * Created by krishna on 01/04/16.
  */
object GroundTruth {
  def data(sc :SparkContext):DataFrame ={
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .load("/Users/krishna/Dropbox/DMKM/Course/SEM2/DataPreprocessing/Anc/articles_authors_disambiguated.csv")

    val df2 = df.withColumnRenamed("C0","id1GT").withColumnRenamed("C1","id2GT").withColumnRenamed("C2","dGT")
        .withColumnRenamed("C3","authorNameGT").withColumnRenamed("C4","authorFullGT")

    return df2.drop("id1GT").drop("authorNameGT")
  }

}
