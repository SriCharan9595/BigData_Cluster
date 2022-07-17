import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object clusterTest {

  def main(args: Array[String]): Unit = {

    println("Hello world!")

    // 1. Spark Config (we have to mention created spark config in import stmt in line 1)
    val sparkConfig = new SparkConf() // creating a spark config
    sparkConfig.setMaster("spark://Sricharan-VirtualBox:7077") // setting up env with created config, either local or cluster
    sparkConfig.setAppName("clusterTest") // naming the project with created config

    // 2. Spark Context (we have to mention created spark context in import stmt in line 1)
    val sparkContext = new SparkContext(sparkConfig) // creating a spark context with spark config

    // 3. Adding Transformations
    val programLang = List("C", "DotNet", "Java", "Angular", "Node")
    val myLang2 = programLang.map ( lang => lang.length )
    myLang2.foreach( lang =>  println(lang) )

    // 5. Close the Spark Context
    sparkContext.stop()

  }
}