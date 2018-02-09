import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import java.net.{ServerSocket,Socket}
import java.io.PrintStream

object HW4 {
	val conf = new SparkConf().setMaster("local[7]").setAppName("test")
	val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	import sqlContext.implicits._
	val base = "/Users/yangmei/eclipse-workspace/hw/src/WordCount/"
	val inputFile = base + "par"
//	val inputFile = "hdfs://cluster01:8020/dblp/par"
	//执行一次即可
//	{
//		print("test1")
//		val sourceFile = base + "dblp.xml"
//		val processedFile = base + "dblp1"
//		val parquetFile = base + "par"
//
//		var t = sc.textFile(sourceFile)
//			.map(_.replaceAll("</inproceedings>|</mastersthesis>|</phdthesis>", "</article>").replaceAll("<inproceedings|<mastersthesis|<phdthesis", "<article"))
//		t.saveAsTextFile(processedFile)
//
//		val articleDf = sqlContext.read
//			.format("com.databricks.spark.xml")
//			.option("rowTag", "article")
//			.option("excludeAttribute", true)
//			.option("treatEmptyValuesAsNulls", true)
//			.load(processedFile)
//
//		val articlesDf = articleDf.select("title", "author", "year").filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))
//
//		articlesDf.write.parquet(parquetFile)
//		print("test2")
//	}

	val articlesDf = sqlContext.read.parquet(inputFile)
	articlesDf.cache()

	def author(name: String): Unit ={
		val articles = articlesDf.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains(name.toLowerCase))
			.sort($"year".desc)
			.map(_.getString(0))
			.collect
		val count = articles.size
		println(count)
		articles.foreach(println)
	}

	def article_sum (name : String): Long = {
		articlesDf.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains(name.toLowerCase)).count
	}

	def coauthor(name : String): Unit= {
		val authors = articlesDf.filter(row => row.getSeq[String](1).map(_.toLowerCase).contains(name.toLowerCase))
			.collect
			.flatMap(row => row.getSeq[String](1))
			.distinct
			.map(an => (an, article_sum(an)))
	  	.sortBy(-_._2)

		println(authors.size)
		authors.foreach(t => println(t._1 + " : " + t._2))
	}

	def clique(names : String): Unit ={
		try {
			var authors = names.split(",").map(_.trim.toLowerCase.replaceAll(" +", " "))
			var tmp = authors.last
			var number = tmp.slice(tmp.indexOf("[")+1, tmp.indexOf("]")).toLong
			authors(authors.length-1) = tmp.slice(0, tmp.indexOf("["))

			if(authors.length >= 1){
				val articles = articlesDf.filter(row => authors.toSeq.diff(row.getSeq[String](1).map(_.toLowerCase)).size == 0).sort($"year".desc).collect
				val count = articles.size
				if (count >= number){
					println("YES")
				}else{
					println("NO")
				}
				articles.foreach(row => println(row.getString(0)+ " , ["+row.getSeq[String](1).mkString(",")+"] , "+row.getLong(2)))
			} else {
				println("invalid input")
			}
		} catch {
			case _: Throwable =>
		}

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
		val ssc = new StreamingContext(sc, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 3982)
		var inputs = lines.map(_.split(":").map(_.trim.replaceAll(" +", " ")))
		  	.foreachRDD(rdd => {
					if (!rdd.isEmpty){
						rdd.foreach(arr =>
							if (arr.length >= 2) {
								println(arr(0) + ":" + arr(1))
								proxy(arr(0).trim.toLowerCase, arr(1).trim.toLowerCase)
							} else {
								print("invaid input")
							}
						)
					} else {
//						print(".")
					}
				})
		ssc.start()
		ssc.awaitTermination()
	}
}