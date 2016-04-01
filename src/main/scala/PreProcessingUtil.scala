import com.rockymadden.stringmetric.phonetic._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by krishna on 16/03/16.
  */
object PreProcessingUtil {

  // Remove Accents
  val stringNormalizer = udf((s: String) => StringUtils.stripAccents(s))

  def textPreprocess(c: Column) ={
    lower(trim(c))
  }

  // Lower , Remove Punctuations , Split by ;
  def splitAuthor(c: Column) = split(
    regexp_replace(lower(trim(c)), "[^a-zA-Z0-9;\\s]", ""), ";"
    //regexp_replace(lower(trim(c)), "[^a-zA-Z0-9;\\s]", ""), ";"
  )

  // My Custom trim function
  val custT = udf((s: Seq[String]) => custTrim(s))
  def custTrim(name : Seq[String]) ={
    name.map(s => s.trim)
  }

  val authorD  = udf((s: Seq[String]) => authorDistance(s))
  def authorDistance(name : Seq[String]) ={ name.zipWithIndex.map(_._2)}

  // Zips ID and Author together
  val zip = udf((xs: Seq[Int], ys: Seq[String]) => xs.zip(ys))

  val authorR  = udf((al: String,a:String) => authorRem(al,a))
  def authorRem(authorlist : String,author :String) = {
    authorlist.split(",").filter(_ !=author)
  }


  val stringN = udf((s: String) => NysiisAlgorithm.compute(s))
  //val stringRN = udf((s: String) => RefinedNysiisAlgorithm.compute(s))
  //val stringRS = udf((s: String) => RefinedSoundexAlgorithm.compute(s))
  //val stringM = udf((s: String) => MetaphoneAlgorithm.compute(s))
  //val stringS = udf((s: String) => SoundexAlgorithm.compute(s))

}

/*
// Remove duplicate co-authors
  val authorR  = udf((al: Seq[String],a:String) => authorRem(al,a))
  def authorRem(authorlist : Seq[String],author :String) = {
    authorlist.filter(_ !=author)
  }
 */