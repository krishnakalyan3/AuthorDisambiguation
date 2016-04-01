import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by krishna on 30/03/16.
  */
object Statistics {



  def printVal(val1 : Long, val2 : String) ={
    println("Total "+val2 +  " ")
    println(val1)
  }

  def stat(featuers : DataFrame,col1 : String) ={
    //val countRecods =featuers.select(col).count()
    //val countDistinctRec = featuers.select(col1).distinct().count()

    featuers.groupBy("phonetic").agg(count("*").alias("cnt"))
      .orderBy(desc("cnt"))
      .show()

    //printVal(countRecods,"Record Count")
    //printVal(countDistinctRec,col1)

    //case class Test(id: Int, d:Int, title: String, journal: String, year: Int  )

    /*
       transform11.groupBy("phonetic").agg(count("*").alias("MetaphoneAlgorithm")).orderBy(desc("cnt"))
         .show()

       transform11.groupBy("phonetic1").agg(count("*").alias("SoundexAlgorithm")).orderBy(desc("cnt"))
         .show()

       transform11.groupBy("phonetic2").agg(count("*").alias("NysiisAlgorithm")).orderBy(desc("cnt"))
         .show()

       transform11.groupBy("phonetic3").agg(count("*").alias("RefinedNysiisAlgorithm")).orderBy(desc("cnt"))
         .show()

       transform11.groupBy("phonetic4").agg(count("*").alias("RefinedSoundexAlgorithm")).orderBy(desc("cnt"))
         .show()

   */

    //val block1 = Statistics.stat(feature_matrix,"phonetic")
    //val block2 = Statistics.stat(feature_matrix,"phonetic1")
    //val block3 = Statistics.stat(feature_matrix,"phonetic2")
    //val block4 = Statistics.stat(feature_matrix,"phonetic3")
    //val block5 = Statistics.stat(feature_matrix,"phonetic4")


    //.show()
    //featuers.printSchema()
    //featuers.filter("phonetic = 'lk'").show(3)
    //featuers.filter("phonetic = 'l'").show(3)


  }

}
