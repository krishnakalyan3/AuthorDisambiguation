import PreProcessingUtil._
import org.apache.spark.sql.functions.col
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

    // Articles
    val df_article = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Keywords
    val df_keyword = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "kw_new")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Id,Author,Article,title,distance
    val author = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "kw_new")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Joining Author and Keywords
    val df_article1 = df_article.select("id","authors","title","journal","year").alias("article")
    val df_keyword1 = df_keyword.select("id", "kw").alias("kw")

    val transform0 = df_article1.join(df_keyword1,col("article.id")===col("kw.id"),"left_outer")
      .select("article.id","authors","title","journal","year","kw")

    // Remove Author Accents
    val transform1 = transform0.withColumn("noaccent", stringNormalizer(transform0("authors")))

    // Lower case , remove punctuations , split wrt ;
    val transform2 = transform1.withColumn("splitauthor", splitAuthor(transform1("noaccent"))).drop("noaccent")


    // Split author convert to array
    val transform3 = transform2.withColumn("distance", authorD(transform2("splitauthor")))
    transform3.show(3)




    // Rearranging the name "m ranjan" -> "rajan m" helps with
    //val transform3 = transform2.withColumn("authorR", authorRearrange(transform2("splitauthor")))
    //transform3.show(3)
    // .withColumn("sigID", monotonicallyIncreasingId)
    // Explode authors
    //val transform3 = transform2.withColumn("Author", explode(transform2("splitauthor")))
    //ex
    //transform0.show(3)
    //transform2.printSchema()
    //val explode_author = flat_author.withColumn("Author", explode(flat_author("authorsSplit")))
    //explode_author.select("authorsSplit","Author").show(3)
    //val countdistance = flat_author.withColumn("D", stringNormalizer(df_article("authors")))
    //val countdistance= flat_author.printSchema()
    //val temp  = countdistance.show(3)
    //val op =explode_author.select("authorsSplit").show(3)

    return transform2
  }







}


