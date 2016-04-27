import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Created by geo on 27.04.16.
  */
object SqlT {
  case class Human(name: String, age:Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val rd = sc.parallelize(
      List(
        Human("Ion",20),
        Human("Marie", 21),
        Human("Ninja", 40)
      )
    )
    val humanDf = rd.toDF()

    val olderThan40 = humanDf.filter(humanDf("age") > 21)
    olderThan40.show()
  }
}
