/**
  * Created by geo on 26.04.16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object GroupByExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val customers = CustomerLoader.load(sc, "/home/geo/IdeaProjects/SparkyOne/src/customers.csv")

    val groupByZip = customers.groupBy { c => c.zip}
    groupByZip.collect.foreach { e=> {
      val zipCode = e._1
      println(s"code is: $zipCode")
      println("customers:" + e._2.mkString(","))
    }}
  }
}
