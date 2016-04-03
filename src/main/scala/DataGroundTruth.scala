import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by krishna on 01/04/16.
  */
object DataGroundTruth {

  def data(sc :SparkContext):DataFrame ={


    val data =ReadFromCSV.readFreomCSV(sc,"/Users/krishna/Dropbox/DMKM/Course/SEM2/DataPreprocessing/articles_authors_disambiguated.csv","true")

    val df2 = data.withColumnRenamed("authorid","id1GT").withColumnRenamed("id","id2GT").withColumnRenamed("d","dGT")
        .withColumnRenamed("author","authorNameGT").withColumnRenamed("completename","authorFullGT")



    return df2.drop("id1GT").drop("authorNameGT")
  }

}
