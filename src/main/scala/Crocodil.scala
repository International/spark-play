/**
  * Created by geo on 26.04.16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Crocodil {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val lines= sc.textFile("/home/geo/IdeaProjects/SparkyOne/src/customers.csv")

    case class Customer(name: String, age: Int, gender: String, zip: String)

    val customers = lines.map(l => {
      val a = l.split(",")
      Customer(a(0), a(1).toInt, a(2), a(3))
    })

    val groupByZip = customers.groupBy { c => c.zip}
    groupByZip.collect.foreach { e=> {
      val zipCode = e._1
      println(s"code is: $zipCode")
      println("customers:" + e._2.mkString(","))
    }}
  }
}
