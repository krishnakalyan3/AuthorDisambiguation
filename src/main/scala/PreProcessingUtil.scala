import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
/**
  * Created by krishna on 16/03/16.
  */
object PreProcessingUtil {

  // Remove Accents
  val stringNormalizer = udf((s: String) => StringUtils.stripAccents(s))

  // Lower , Remove Punctuations , Split by ;
  def splitAuthor(c: Column) = split(
    regexp_replace(lower(c), "[^a-zA-Z0-9;\\s]", ""), ";"
  )

  val authorArrange = udf((s: List[String]) => sortName(s))

  def sortName(s: List[String]) ={
    s.sortBy(_.length).map { name => name.split(" ").sortWith(_.length > _.length).mkString(" ")}
  }

  def main(args: Array[String]) {
  println(sortName(List("LK Advani","Krishna Kalyan")))
  }

}
