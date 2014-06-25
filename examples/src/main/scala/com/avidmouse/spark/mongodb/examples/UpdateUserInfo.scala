package com.avidmouse.spark.mongodb.examples

import org.apache.spark.{SparkContext, SparkConf}

import com.avidmouse.spark.mongodb.Mongo
import com.avidmouse.spark.mongodb.sparkContext._
import com.avidmouse.spark.mongodb.rdd._

import play.api.libs.json._

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
object UpdateUserInfo {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: UpdateUserInfo <mongoURI> <userCollection>")
      System.exit(1)
    }

    val Array(mongoUri, userCollection) = args

    val collection = Mongo(mongoUri).collection(userCollection)

    val sparkConf = new SparkConf().setAppName("updateUserInfo")
    // Get a SparkContext
    val sc = new SparkContext(sparkConf)

    val users = sc.mongoRDD(collection.find(Json.obj(), Json.obj("age" -> 1)))

    users.map { user =>
      val age = (user \ "age").as[Int] + 1
      ((user \ "_id"), Json.obj("$set" -> Json.obj("age" -> age)))
    }.saveAsMongoDocument(collection)
  }

}
