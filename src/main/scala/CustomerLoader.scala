import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by geo on 26.04.16.
  */
object CustomerLoader {
  def load(sc: SparkContext, filePath: String): RDD[Customer] = {
    val lines = sc.textFile(filePath)

    val customers = lines.map(l => {
      val a = l.split(",")
      Customer(a(0), a(1).toInt, a(2), a(3))
    })

    customers
  }
}
