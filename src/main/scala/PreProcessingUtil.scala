import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.split

/**
  * Created by krishna on 16/03/16.
  */
object PreProcessingUtil {

  def split_author(c: Column) = split(
    c,";"
  )



}
