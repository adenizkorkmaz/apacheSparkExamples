import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by denizkorkmaz on 02/06/15.
 */
object Songs {


  def main (args: Array[String]): Unit = {

    case class Grade(ID: Int, gradeType: String, score: Double)

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val gradeFile = sc.textFile("/Users/denizkorkmaz/Desktop/unique_tracks.txt")
    println(gradeFile.count())

    val splited = gradeFile.flatMap(_.split("<SEP>"))
    val wordCount = splited.map(x => (x,1)).reduceByKey(_+_)
    wordCount.collect().foreach(println)


    val lineCountByLineLegnht = gradeFile.map(line => (line.length, 1)).reduceByKey(_+_)
    lineCountByLineLegnht.collect().foreach(println)

  }

}
