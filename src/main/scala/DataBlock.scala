import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * Created by krishna on 03/04/16.
  */
object DataBlock {
  def block(dataMatrix : DataFrame):DataFrame = {

    //val transform2 = dataMatrix.filter("phonetic = 'caracacsp'")
    //val transform1 = dataMatrix.groupBy(col("phonetic")).agg(sum)
    //val transform1 = dataMatrix.filter("authorFullGT is not null")
    val transform2 = dataMatrix.as("t1").join(dataMatrix.as("t2"),col("t1.sigID") !== col("t2.sigID"),"inner")


  return transform2
  }

}
