package com.avidmouse.spark.streaming.mongodb

import com.avidmouse.spark.mongodb.Mongo
import com.avidmouse.spark.mongodb.rdd._

import org.apache.spark.streaming.dstream.DStream

import play.api.libs.json.JsValue

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
object MongoSparkDStream {

  class InsertDStream[A <: JsValue](stream: DStream[A]) {

    def saveAsMongoDocument(col: Mongo.Collection) {
      stream.foreachRDD(_.saveAsMongoDocument(col))
    }
  }

  class UpdateDStream[A <: JsValue, B <: JsValue](stream: DStream[(A, B)]) {

    def saveAsMongoDocument(col: Mongo.Collection, upsert: Boolean = true, multi: Boolean = false) {
      stream.foreachRDD(_.saveAsMongoDocument(col, upsert, multi))
    }
  }

  object Implicits {

    import scala.language.implicitConversions

    implicit def asInsertDStream[A <: JsValue](stream: DStream[A]) = new InsertDStream(stream)

    implicit def asUpdateDStream[A <: JsValue, B <: JsValue](stream: DStream[(A, B)]) = new UpdateDStream(stream)
  }

}
