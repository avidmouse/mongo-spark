package com.avidmouse.spark.mongodb

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration

import com.mongodb.hadoop.MongoInputFormat
import org.bson.BSONObject

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
class MongoSparkContext(sc: SparkContext) {
  def mongoRDD(qb: Mongo.Collection.QueryBuilder) = {
    val config = new Configuration
    config.set("mongo.input.uri", qb.collection.outputUri)
    qb.queryOpt.foreach(q => config.set("mongo.input.query", q.toString()))
    qb.projectionOpt.foreach(p => config.set("mongo.input.fields", p.toString()))
    qb.sortOpt.foreach(s => config.set("mongo.input.sort", s.toString()))
    qb.limitOpt.foreach(l => config.set("mongo.input.limit", l.toString))
    qb.skipOpt.foreach(s => config.set("mongo.input.skip", s.toString))
    sc.newAPIHadoopRDD(config, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject]).map(
      arg => BSONFormats.toJSON(arg._2)
    )
  }
}

object MongoSparkContext {

  object Implicits {

    implicit def asMongoSparkContext(sc: SparkContext) = new MongoSparkContext(sc)
  }

}
