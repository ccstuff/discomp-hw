import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object HW3 {
	val inputFile = "/Users/yangmei/eclipse-workspace/hw/src/WordCount/top/part-00000"
	val conf = new SparkConf().setMaster("local[2]").setAppName("top").set("spark.executor.memory", "8g")
		val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	import sqlContext.implicits._
	val articleDf = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "article").option("excludeAttribute", true).option("treatEmptyValuesAsNulls", true).load(inputFile)
	val articles = articleDf
		.filter("year >= 2000 and (url like '%conf/sigmod%' or url like '%conf/icde%' or url like '%journals/pvldb%')")
		.select("title", "author", "year")
  	.filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))

	articles.cache
	def top_author(): Unit ={
		val rdd = articles.select("author").map(row => row.getSeq[String](0).toArray.map(name => (name,1))).flatMap(arr => arr).rdd.reduceByKey((a,b) => a+b)
		val arr = rdd.collect
		val top100author = arr.sortWith(_._2 > _._2).slice(0, 100)
		sc.parallelize(top100author.map(t => t._1)).coalesce(1).saveAsTextFile("/Users/yangmei/Desktop/2017-2018-1/discomp/hw3/authors")
		sc.parallelize(top100author).coalesce(1).saveAsTextFile("/Users/yangmei/Desktop/2017-2018-1/discomp/hw3/authors_full")
		print("author done")
	}
	def top_words(): Unit ={
		val stopwords = sc.textFile("/Users/yangmei/eclipse-workspace/hw/src/WordCount/stopwords.txt").collect().map(_.toLowerCase)
		val words = articles.select("title").map(row => row.getString(0).toLowerCase().dropRight(1).split(" ").diff(stopwords).map(a => (a,1))).flatMap(t => t).rdd.reduceByKey((a,b) => a+b)
		val arr = words.collect

		val top100words = arr.sortWith(_._2 > _._2).slice(0, 110)
		sc.parallelize(top100words.map(t => t._1)).coalesce(1).saveAsTextFile("/Users/yangmei/Desktop/2017-2018-1/discomp/hw3/words")
		sc.parallelize(top100words).coalesce(1).saveAsTextFile("/Users/yangmei/Desktop/2017-2018-1/discomp/hw3/words_full")
		print("words done")
	}

	//run first
	def pre(): Unit ={
		var t = sc.textFile("/Users/yangmei/eclipse-workspace/hw/src/WordCount/dblp.xml").map(_.replaceAll("</inproceedings>", "</article>").replaceAll("<inproceedings", "<article"))
		t.coalesce(1).saveAsTextFile("/Users/yangmei/eclipse-workspace/hw/src/WordCount/top/")
	}

	def main(args: Array[String]): Unit = {
		print("begin")
		top_author()
		top_words()
	}
}
