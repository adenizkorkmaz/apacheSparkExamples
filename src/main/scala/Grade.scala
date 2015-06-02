import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read

/**
 * Created by denizkorkmaz on 29/05/15.
 */



object Grade {

  implicit val formats = Serialization.formats(NoTypeHints)


  def main (args: Array[String]): Unit = {

    case class Grade(ID: Int, gradeType: String, score: Double)

    case class GradeInput(id: String, student_id: Int, typee: String, score: Double)

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(conf,Seconds(1))

//    val gradeFile2 = ssc.textFileStream("/Users/denizkorkmaz/Desktop/grade.js")
    val gradeFile = sc.textFile("/Users/denizkorkmaz/Desktop/grade.js")

    val splited = gradeFile.flatMap(_.split("[ :,{}]"))
    val wordCount = splited.map(x => (x,1)).reduceByKey(_+_)

    val exam = gradeFile.filter(x => x.contains("exam"))
    val hw = gradeFile.filter(x => x.contains("homework"))
    val quiz = gradeFile.filter(x => x.contains("quiz"))

    //exam geçen satırları al. Her birinin uzunluğunu bul. Bunların count bazında grupla.
    val examLineCount = exam.map(x => (x.length, 1)).reduceByKey(_+_)
    examLineCount.collect().foreach(println)

    //Object'ye map et
    val grade = gradeFile.map(_.split("[,:}]"))
                         .map(p => Grade(p(5).trim.toInt,p(7),p(9).trim.toDouble))


    gradeFile.map(x => read[GradeInput](x)).foreach(out => println(out.score))




    // notu 50 den fazla olanları getir
    val scoreCmp = grade.filter(t => t.score > 50)
    scoreCmp.collect().foreach(println)

    //examden 90 dan fazla alanları getir
    val typeAndScoreCmp = grade.filter(t => t.gradeType.contains("exam")).filter(t => t.score>90)
    typeAndScoreCmp.collect().foreach(println)
    println(typeAndScoreCmp.count())

    //id=160 olan öğrencinin tüm notları
    val byId = grade.filter(t => t.ID == 160).map(t => (t, 1)).reduceByKey(_+_).collect().foreach(println)


  }
}
