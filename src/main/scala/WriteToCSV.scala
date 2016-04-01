import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/**
  * Created by krishna on 28/03/16.
  */
object WriteToCSV {


  def writetoCSV(data : DataFrame,sc : SparkContext, pathloc : String)={

    data.write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(pathloc)


  }

}
