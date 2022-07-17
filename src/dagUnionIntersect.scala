import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object dagUnionIntersect {

  def main(args : Array[String]) = {

    val sparkSession = SparkSession.builder()
      .appName("dagUnionIntersection")
      .master("spark://Sricharan-VirtualBox:7077")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val textFileRDD = sc.textFile("/home/cheri/Downloads/auth.csv")

    textFileRDD.union(textFileRDD).union(textFileRDD).union(textFileRDD).intersection(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).intersection(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).intersection(textFileRDD).union(textFileRDD)
      .intersection(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).intersection(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD).intersection(textFileRDD)
      .union(textFileRDD).union(textFileRDD).intersection(textFileRDD).union(textFileRDD).union(textFileRDD)
      .union(textFileRDD).intersection(textFileRDD).union(textFileRDD).union(textFileRDD).union(textFileRDD)
      .collect().foreach(println)

    //csvDataDF.select("auth_code","subreq_id","aua","sa").show()

  }

}
