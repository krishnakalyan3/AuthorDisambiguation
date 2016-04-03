import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import PreProcessingUtil._


/*
* Summary
* Articles Records : 3,18,591 rows
* Columns id,title,journal,year,kw,authors,d,co-author,phonetic,sigID
* */

object Features {

  private val AppName = "Data PreProcessing"

  def features(sc :SparkContext):DataFrame ={

    val sqlContext = new SQLContext(sc)
    val sqlContext1: SQLContext = new HiveContext(sc)

    // id, subject
    val df_assoication = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "subject_asociations")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Authors - id,d,author
    val df_author = sqlContext1.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles_authors")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Articles - id, authors, title, journal, type, numrefs, times_cited, doi, year, volume, issue, BE, EP, abbr,ut, nsr
    val df_article = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    // Keywords - id, type_keyw, keyword
    val df_keyword = sqlContext1.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles_keywords_clean_sel")
      .option("user", "root")
      .option("password", "dmkm1234").load()
    val df_keyword1 =df_keyword.groupBy(col("id")).agg(concat_ws(",", collect_list(col("keyword"))).alias("keyword"))

    // Institution - id, d1, d2, institution
    val insti = sqlContext1.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles_institutions")
      .option("user", "root")
      .option("password", "dmkm1234").load().select("id","d1","institution")
    val insti1 =insti.groupBy(col("id"),col("d1")).agg(concat_ws(",", collect_list(col("institution"))).alias("institution"))

    // Join df_author + df_article
    val transform0 =df_author.as("author").join(df_article.as("article"),col("author.id")<=>col("article.id"),"left_outer")
      .drop(col("article.id")).drop(col("article. authors")).drop(col("type")).drop(col("numrefs")).drop(col("times_cited"))
      .drop(col("doi")).drop(col("volume")).drop(col("issue")).drop(col("BP")).drop(col("EP")).drop(col("abbr"))
      .drop(col("ut")).drop(col("nsr")).drop(col("authors"))

    // Join transform0 + df_keyword1
    val transform1 = transform0.as("trans0").join(df_keyword1.as("keyword"),col("trans0.id")<=>col("keyword.id"),"leftouter")
      .drop(col("keyword.id"))

    // Join transform1 + insti1
    val transform2 = transform1.as("t1").
      join(insti1.as("insti"),
        (col("t1.id")<=>col("insti.id"))
          &&
          (col("d")<=>col("d1")),
        "leftouter").drop(col("d1")).drop(col("insti.id"))

    // id2GT + authorFullGT
    val groundtruth = DataGroundTruth.data(sc)



    // author - remove accent, lower, trim
    val transform3 = transform2.withColumn("author1", textPreprocess(stringNormalizer(col("author")))).drop(col("author"))

    // co-author
    val transform4 = transform3.groupBy(col("id")).agg(concat_ws(",", collect_list(col("author1"))).alias("coauthor"))

    // Join transform3 + transform4
    val transform5 = transform3.as("t3").join(transform4.as("t4"),col("t3.id")<=>col("t4.id"),"leftouter").drop(col("t4.id"))
    //transform5.show(3)

    // Generate Co-Authors
    val transform6 = transform5.withColumn("coauthor",authorR(col("coauthor"),col("author1")))
    //transform6.show(3)

    // Double NysiisAlgorithm
    val transform7 = transform6.withColumn("phonetic",stringN((col("author1"))))



        // Ground truth data - Current INNER
        val transform8 = transform7.as("t7").join(groundtruth.as("gt"),((col("t7.id") <=> col("gt.id2GT")) &&
          (col("t7.d") <=> col("gt.dGT"))),"inner").drop(col("gt.id2GT")).drop(col("gt.dGT"))

        // Signature
        val transform9 = transform8.withColumn("sigID", monotonicallyIncreasingId).withColumnRenamed("author1","author")

        val transform10 = transform9.as("t9").join(df_assoication.as("dfasc"),col("t9.id") <=> col("dfasc.id"),"leftouter")
          .drop(col("dfasc.id"))

/*
            //val block1 = Statistics.stat(transform8,"phonetic")


            //transform8.show(3)
            //
            /*
            // Joining Author and Keywords
            val df_article1 = df_article.select("id","authors","title","journal","year").alias("article")
            val df_keyword1 = df_keyword.select("id", "kw").alias("kw")

            val transform0 = df_article1.join(df_keyword1,col("article.id")<=>col("kw.id"),"left_outer")
              .select("article.id","authors","title","journal","year","kw")

            // Remove Author Accents
            val transform1 = transform0.withColumn("noaccent", stringNormalizer(col("authors")))

            // Lower case , remove punctuations , split wrt ; **Trim
            val transform2 = transform1.withColumn("splitauthor", custT(splitAuthor(col("noaccent")))).drop("noaccent")

            // Split author convert to array of numbers
            val transform3 = transform2.withColumn("distance", authorD(col("splitauthor")))


            //exploding - distance and splitauthor
            val transform4 =transform3.withColumn("value",explode(zip(col("distance"),col("splitauthor"))))
              .select(col("id"),col("title"),col("journal"),col("year"),col("kw"),
                col("splitauthor"),col("value._2").alias("author"),col("value._1").alias("d"))

            // Removing duplicate co-authors
            val transform5 = transform4.withColumn("coauthor",authorR(col("splitauthor"),col("author"))).drop("splitauthor")



            // join transform_6 with id,d +  insti with id, d2
            val transform7 = transform6.as("t7").
              join(insti1.as("insti"),
                (col("t7.id")<=>col("insti.id"))
                  &&
                  (col("d")<=>col("d1")),
                "leftouter")

            // Adding signatures

            transform8.show(30)


            //transform8.registerTempTable("transform7")
            //val results =sqlContext.sql("SELECT count(*) FROM transform7")
            //results.show()

            //transform8.show(10)

            //val transform8 = transform7.select("phonetic").rdd.distinct.count

            //transform7.registerTempTable("transform7")
            //val results =sqlContext.sql("SELECT count(distinct phonetic) FROM transform7")
            //results.show()

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
        */

          */
    return transform10
  }







}


