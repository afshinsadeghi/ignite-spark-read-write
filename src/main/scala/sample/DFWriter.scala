package sample

import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.{IgniteContext, IgniteDataFrameSettings, IgniteRDD}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.ml.math.primitives.vector

object DFWriter extends App {
  //  def main(args: Array[String]) {
  // create the spark context

  /* implicit val spark = SparkSession.builder()
     .appName("Example Program")
     .master("local")
     .config("spark.executor.instances", "1")
     .getOrCreate()
*/
  //val igConfig = new IgniteConfiguration()).fromCache("resultsIgniteRDD"))
  private val conf = new SparkConf()
    .setAppName("IgniteRDDExample")
    .setMaster("local")
    .set("spark.executor.instances", "2")

  // Spark context.
  val sc = new SparkContext(conf)

  //val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
  val igniteContext = new IgniteContext(sc, () => new IgniteConfiguration(), false)

  val igniteWordCacheRDD: IgniteRDD[Int, Int] = igniteContext.fromCache[Int, Int]("sharedRDD")
  val igniteEmbeddingRDD: IgniteRDD[Int, Double] = igniteContext.fromCache[Int, Double]("sharedRDD")

  //val tokenized = sc.textFile("/data/test.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

  val sharedRDD: IgniteRDD[Int, Int] = igniteContext.fromCache[Int, Int]("sharedRDD")

  // Fill the Ignite Shared RDD in with Int pairs.
  sharedRDD.savePairs(sc.parallelize(1 to 100000, 1).map(i => (i, i)))


  // Transforming Pairs to contain their Squared value.
  sharedRDD.mapValues(x => (x * x))

  // Retrieve sharedRDD back from the Cache.
  val transformedValues: IgniteRDD[Int, Int] = igniteContext.fromCache("sharedRDD")

  // Perform some transformations on IgniteRDD and print.
  val squareAndRootPair = transformedValues.map { case (x, y) => (x, Math.sqrt(y.toDouble)) }

  println(">>> Transforming values stored in Ignite Shared RDD...")

  // Filter out pairs which square roots are less than 100 and
  // take the first five elements from the transformed IgniteRDD and print them.
  squareAndRootPair.filter(_._2 < 100.0).take(5).foreach(println)


  // check for ml/nn class https://ignite.apache.org/releases/2.7.6/javadoc/org/apache/ignite/ml/nn/Activators.html:
  // also check vectors https://ignite.apache.org/releases/2.7.6/javadoc/org/apache/ignite/ml/math/primitives/vector/Vector.html

  // 	kNorm of vectors are usable
  val myVec = Vector(112.34,23.34,12.4,34.675)
  // ....

  // Execute a SQL query over the Ignite Shared RDD.
  //val df = transformedValues.sql("select * from Integer where _val < 100 and _val > 9 ")
  //df.show(4)
  // Show ten rows from the result set.

  // Close IgniteContext on all workers.
  igniteContext.close(true)

  // Stop SparkContext.
  sc.stop()
}