import org.apache.spark.ml.feature.{StopWordsRemover, Word2Vec}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.sql.functions.{lower, regexp_replace, split, udf}
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * TO DO: get dataframe of id, Title , Author ,keywords
  *
  */
object LoadData {

  private val AppName = "Data PreProcessing"
  private val threads =4



  def main(args: Array[String]) {

    val master_threads = "local["+threads+"]"
    val conf = new SparkConf().setAppName(AppName)
      .setSparkHome("/Users/krishna/spark")
      .setMaster(master_threads)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df_article = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:8889/dmkm_articles")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "articles_1")
      .option("user", "root")
      .option("password", "dmkm1234").load()

    def tokenize(c: Column) = split(
      regexp_replace(lower(c), "[^a-zA-Z0-9\\s]", ""), "\\s+"
    )

    //df_article.select(tokenize(df_article("title"))).show(3)

    val myToken = udf((xs: String) => xs.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+"))
    val dftoken=  df_article.withColumn("token", myToken(df_article("title")))

    val remover: StopWordsRemover = new StopWordsRemover()
      .setInputCol("token")
      .setOutputCol("processed_title")



    val nostop = remover.transform(dftoken).drop("token")
    val word2Vec = new Word2Vec()
      .setInputCol("processed_title")
      .setOutputCol("w2v")


    val model1= word2Vec.fit(nostop)
    //model1.findSynonyms("statistics", 40).show(40)
    //model1.findSynonyms("statistics", 40).show(41)

    //model1.save("/Users/krishna/Dropbox/DMKM/Course/SEM2/model1")
    //val load_model1 = Word2VecModel.load("/Users/krishna/Dropbox/DMKM/Course/SEM2/model1/")
   // val load_model1 =  Word2VecModel.load("/Users/krishna/Dropbox/DMKM/Course/SEM2/model1")
    model1.transform(nostop).take(3).foreach(println)

    val numTopics = 3
    val lda = new LDA().setK(numTopics).setMaxIterations(10)


  }





}
