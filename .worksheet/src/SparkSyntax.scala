import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkSyntax {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(175); 
  println("Welcome to the Scala worksheet");$skip(72); 
  
  val conf = new SparkConf().setMaster("local").setAppName("My App");System.out.println("""conf  : org.apache.spark.SparkConf = """ + $show(conf ));$skip(34); 

	val sc = new SparkContext(conf);System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(101); 

	val input = sc.textFile("D:\\RnD\\spark-labs\\spark-labs\\src\\main\\resources\\wordcount_in.txt");System.out.println("""input  : org.apache.spark.rdd.RDD[String] = """ + $show(input ));$skip(81); 

	// Split it up into words.
	val words = input.flatMap(line => line.split(" "));System.out.println("""words  : org.apache.spark.rdd.RDD[String] = """ + $show(words ));$skip(115); 
	
	// Transform into pairs and count.
	val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y};System.out.println("""counts  : org.apache.spark.rdd.RDD[(String, Int)] = """ + $show(counts ));$skip(170); 
	
	// Save the word count back out to a text file, causing evaluation.
	counts.saveAsTextFile("D:\\RnD\\spark-labs\\spark-labs\\src\\main\\resources\\wordcount_out.txt")}
  
}
