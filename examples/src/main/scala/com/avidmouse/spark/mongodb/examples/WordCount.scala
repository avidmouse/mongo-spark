package com.avidmouse.spark.mongodb.examples

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import com.avidmouse.spark.mongodb.Mongo
import com.avidmouse.spark.mongodb.sparkContext._
import com.avidmouse.spark.mongodb.rdd._

import play.api.libs.json._

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
object WordCount {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: WordCount <mongoURI> <commentsCollection> <countCollection>")
      System.exit(1)
    }

    val Array(mongoUri, commentsCollection, countCollection) = args

    val mongo = Mongo(mongoUri)

    val sparkConf = new SparkConf().setAppName("WordCount")
    // Get a SparkContext
    val sc = new SparkContext(sparkConf)

    val words = sc.mongoRDD(mongo.collection(commentsCollection).find(Json.obj(), Json.obj("msg" -> 1)))

    val counts = words.flatMap(doc => (doc \ "msg").as[String].split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.map {
      case (word, count) =>
        Json.obj("word" -> word, "count" -> count)
    }.saveAsMongoDocument(mongo.collection(countCollection))
  }
}
