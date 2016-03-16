import PreProcessingUtil._
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* Summary
* Articles Records : 3,18,591 rows
* Columns id,authors,title,kw,journal,year
* TO DO :  Country, Create Co-Authors, articles_subjects, Institution,author
*
* */

object Features {

  private val AppName = "Data PreProcessing"

  def features(args: Int):DataFrame ={

    val master_threads = "local["+args+"]"
    val conf = new SparkConf().setAppName(AppName)
      .setSparkHome("/Users/krishna/spark")
      .setMaster(master_threads)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Load Articles
    val df_article = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Load Keywords
    val df_keyword = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "kw_new")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    val concat_array = udf((xs: Seq[String], sep: String) => xs.mkString(sep))

    val df_article1 = df_article.select("id","authors","title","journal","year").alias("article")
    val df_keyword1 = df_keyword.select("id", "kw").alias("kw")

    val feature_matrix = df_article1.join(df_keyword1,col("article.id")===col("kw.id"),"left_outer")
      .select("article.id","authors","title","journal","year","kw")


    val flat_author = feature_matrix.withColumn("authorsSplit", split_author(df_article("authors")))
    val explode_author = flat_author.withColumn("Author", explode(flat_author("authorsSplit")))
    //val ex = flat_author.explode("authorsSplit","OriginalAuthor")
    //ex
    //explode_author.select("authorsSplit","Author").show(3)
    //val countdistance = flat_author.withColumn("D", stringNormalizer(df_article("authors")))
    //val countdistance= flat_author.printSchema()
    //val temp  = countdistance.show(3)
    val op =explode_author.select("authorsSplit").show(3)

    return flat_author
  }







}


