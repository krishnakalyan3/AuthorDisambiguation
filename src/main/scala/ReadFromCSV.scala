import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by krishna on 03/04/16.
  */
object ReadFromCSV {

  def readFreomCSV(sc : SparkContext, pathloc : String,header : String):DataFrame={

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", header)
      .load(pathloc)

    return df
  }


}
