import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object HelloWorld{
	def main(args: Array[String]) : Unit = {
			println("Hello World")
			val conf = new SparkConf().setMaster("local").setAppName("My App");
			val sc = new SparkContext(conf);

			val input = sc.textFile("D:\\RnD\\spark-labs\\spark-labs\\src\\main\\resources\\wordcount_in.txt");

			// Split it up into words.
			val words = input.flatMap(line => line.split(" "));

			// Transform into pairs and count.
			val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y};

			// Save the word count back out to a text file, causing evaluation.
			counts.saveAsTextFile("D:\\RnD\\spark-labs\\spark-labs\\src\\main\\resources\\wordcount_out.txt");
	}
}