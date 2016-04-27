import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Created by geo on 27.04.16.
  */
object SqlT {
  case class Human(id: Int, name: String, age:Int)
  case class Weapon(id:Int, humanId: Int, name: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val rd = sc.parallelize(
      List(
        Human(1, "Ion",20),
        Human(2, "Marie", 21),
        Human(3, "Ninja", 40)
      )
    )
    val weaponsDf = sc.parallelize(
      List(
        Weapon(1, 1, "pitchfork"),
        Weapon(1, 2, "wooden rolling pin")
      )
    ).toDF()

    val humanDf = rd.toDF()

    println("filtering humans older than 21")
    val olderThan40 = humanDf.filter(humanDf("age") > 21)
    olderThan40.show()

    println("showing humans that have weapons")
    val humansWithWeapons = humanDf.join(weaponsDf, humanDf("id") ===  weaponsDf("humanId"), "inner")
    humansWithWeapons.show()
  }
}
