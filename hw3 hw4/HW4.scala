import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HW4 {
	val hdfs = "hdfs://148.100.92.156:4000/user/user02/"
//	val inputFile =  hdfs + "test.xml"
//	val inputFile =  hdfs + "processed/part-00000"
//	val inputFile = "/Users/yangmei/eclipse-workspace/hw/src/WordCount/test.xml"
	val inputFile = "/Users/yangmei/eclipse-workspace/hw/src/WordCount/ProcessedDblp.xml/dblp.xml"
//	val conf = new SparkConf().setMaster("spark://148.100.92.156:4477").setAppName("test").set("spark.executor.memory", "4g")
	val conf = new SparkConf().setMaster("local[7]").setAppName("test")
//	.set("spark.executor.memory", "1g")
//	.set("spark.executor.cores", "1")
	.set("spark.driver.cores", "7")
	.set("spark.driver.memory", "10g")
	.set("spark.default.parallelism", "7")

	val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	import sqlContext.implicits._

	val articleDf = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "article").option("excludeAttribute", true).option("treatEmptyValuesAsNulls", true).load(inputFile)
	val articlesDs = articleDf.select("title", "author", "year").filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))
	articlesDs.cache

	def author(name: String): Unit ={
		val articles = articlesDs.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains(name.toLowerCase))
			.sort($"year".desc)
			.select("title", "year")
		val count = articles.count
		println(count)
		articles.foreach(row => println(row.getString(0), row.getLong(1)))
	}

	def article_sum (name : String): Long ={
		articlesDs.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains(name.toLowerCase)).count
	}

	def coauthor(name : String): Unit = {
		val authors = articlesDs.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains(name.toLowerCase))
			.collect
			.flatMap(row => row.getSeq[String](1))
			.distinct
			.map(an => (an, article_sum(an)))
	  	.sortBy(-_._2)

		println(authors.size)
		authors.foreach(t => println(t._1 + " : " + t._2))
	}

	def clique(names : String): Unit ={
		if(names.split(",").map(_.trim.toLowerCase.replaceAll(" +", " ")).length >= 3){
			val articles = articlesDs.filter(row => names.split(",").map(_.trim.toLowerCase.replaceAll(" +", " ")).toSeq.diff(row.getSeq[String](1).map(_.toLowerCase)).size == 0)
			val count = articles.count
			if (count >= 3){
				println("YES")
			}else{
				println("NO")
			}
			articles.sort($"year".desc)
				.foreach(row => println(row.getString(0)+ " , ["+row.getSeq[String](1).mkString(",")+"] , "+row.getLong(2)))
		} else {
			println("invalid input")
		}
	}

	def pre(): Unit ={
		var t = sc.textFile("hdfs://148.100.92.156:4000/user/user02/dblp.xml")
			.map(_.replaceAll("</inproceedings>|</mastersthesis>|</phdthesis>", "</article>").replaceAll("<inproceedings|<mastersthesis|<phdthesis", "<article"))
		t.coalesce(1) .saveAsTextFile(hdfs + "processed")
	}

	def proxy(option: String, params : String): Unit ={
		if (option == "author"){
			author(params)
		} else if(option == "coauthor"){
			coauthor(params)
		} else if(option == "clique"){
			clique(params)
		}
	}

	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext(sc, Seconds(5))
		val lines = ssc.socketTextStream("localhost", 9999)
		var inputs = lines.map(_.split(":").map(_.trim.replaceAll(" +", " ")))
		  	.foreachRDD(rdd => {
					if (!rdd.isEmpty)
						rdd.foreach(arr =>
							if (arr.length >= 2) {
								println(arr(0) + ":" + arr(1))
								proxy(arr(0).trim.toLowerCase, arr(1).trim.toLowerCase)
							}
						)
				})
		ssc.start()
		ssc.awaitTermination()
	}
}
//demo.author("weixiong rao")
//demo.coauthor("weixiong rao")
//demo.clique("Weixiong Rao, Eiko Yoneki, Lei Chen 0002")
//demo.articlesDs.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains("wei".toLowerCase))
