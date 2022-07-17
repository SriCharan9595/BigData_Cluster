import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object aggregateSort {

  def main(args : Array[String]) = {

    val sparkSession = SparkSession.builder()
      .appName("dagUnionIntersection")
      .master("spark://Sricharan-VirtualBox:7077")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val csvFileData = sparkSession.read.option("header","true").csv("/home/cheri/Downloads/auth.csv")
    csvFileData.createOrReplaceTempView("AUTH_TABLE")

    val dataDF = sparkSession.sql("SELECT auth_code, aua, asa from AUTH_TABLE")
    dataDF.show()

    //csvDataDF.select("auth_code","subreq_id","aua","sa").show()

  }

}
