import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

/**
 * Created by denizkorkmaz on 02/06/15.
 */
object GradeJson {

  implicit val formats = Serialization.formats(NoTypeHints)


  def main(args: Array[String]): Unit = {

    case class GradeInput(typee: String, score: Double)
    case class Grades(hede: Array[GradeInput])

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val gradeFile = sc.textFile("/Users/denizkorkmaz/Desktop/jsonDeneme.json")

    //Object'ye map et
    gradeFile.map(x => read[GradeInput](x)).foreach(out => println(out))




    // notu 50 den fazla olanları getir
    //    val scoreCmp = grade.filter(t => t.score > 50)
    //    scoreCmp.collect().foreach(println)
    //
    //    //examden 90 dan fazla alanları getir
    //    val typeAndScoreCmp = grade.filter(t => t.gradeType.contains("exam")).filter(t => t.score>90)
    //    typeAndScoreCmp.collect().foreach(println)
    //    println(typeAndScoreCmp.count())
    //
    //    //id=160 olan öğrencinin tüm notları
    //    val byId = grade.filter(t => t.ID == 160).map(t => (t, 1)).reduceByKey(_+_).collect().foreach(println)


  }
}
