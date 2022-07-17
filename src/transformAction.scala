import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object transformAction {
  def main(args: Array[String]): Unit = {
    compute();
  }
  def compute()= {

    val sparkConfig = SparkSession.builder()
      .appName("CodaJOB")
      .master("local[*]")
      .getOrCreate()

    val sparkContext = sparkConfig.sparkContext

    //    val programLang = List("C", "DotNet", "Java", "Angular", "Node")
    //    println( programLang(3) )
    //    programLang.foreach( lang =>  println(lang) )
    //
    //    val myLang = programLang.map ( lang => lang.toUpperCase )
    //    myLang.foreach( lang =>  println(lang) )

    val textFileRDD = sparkContext.textFile("/home/cheri/Downloads/auth.csv")

    val mappedRDD = textFileRDD.map(each => {
      val columns = each.split(",")
      println(columns(0) + columns(1))
      AuthDTO(columns(7), columns(9))
    })

    //val filteredRDD = mappedRDD.filter(each => each.value.startsWith("000074"))

    mappedRDD.foreach(each=> println(each))
    sparkContext.stop()
  }
  case class AuthDTO(value: Any, value1: Any);
}