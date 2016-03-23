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

  val authorD  = udf((s: Seq[String]) => authorDistance(s))
  def authorDistance(name : Seq[String]) ={ name.zipWithIndex.map(_._2)}


}
