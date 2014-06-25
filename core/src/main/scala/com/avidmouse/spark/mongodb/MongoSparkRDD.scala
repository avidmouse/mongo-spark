package com.avidmouse.spark.mongodb

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration

import play.api.libs.json.JsValue

import com.mongodb.hadoop.MongoOutputFormat

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
object MongoSparkRDD {

  class InsertRDD[A <: JsValue](rdd: RDD[A]) {

    def saveAsMongoDocument(col: Mongo.Collection) {
      val config = new Configuration with Serializable
      config.set("mongo.output.uri", col.outputUri)

      rdd.map {
        json =>
          (null, BSONFormats.toBSON(json))
      }.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[MongoOutputFormat[Any, Any]], config)
    }
  }

  class UpdateRDD[A <: JsValue, B <: JsValue](rdd: RDD[(A, B)]) {

    import com.mongodb.hadoop.io.MongoUpdateWritable
    import org.bson.BasicBSONObject

    def saveAsMongoDocument(col: Mongo.Collection, upsert: Boolean = false, multi: Boolean = false) {
      val config = new Configuration with Serializable
      config.set("mongo.output.uri", col.outputUri)
      rdd.map {
        case (query, update) =>
          (null, new MongoUpdateWritable(BSONFormats.toBSON(query).asInstanceOf[BasicBSONObject], BSONFormats.toBSON(update).asInstanceOf[BasicBSONObject], upsert, multi))
      }.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[MongoOutputFormat[Any, Any]], config)
    }
  }

  object Implicits {

    import language.implicitConversions

    implicit def asInsertRDD[A <: JsValue](rdd: RDD[A]) = new InsertRDD(rdd)

    implicit def asUpdateRDD[A <: JsValue, B <: JsValue](rdd: RDD[(A, B)]) = new UpdateRDD(rdd)
  }

}
