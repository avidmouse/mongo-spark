package com.avidmouse.spark.mongodb

import play.api.libs.json._

import org.bson.{BSONObject, BasicBSONObject}

import scala.collection.convert.decorateAsJava._

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
object BSONFormats {
  def toBSON(json: JsValue): Any = {
    json match {
      case JsNull => null
      case JsBoolean(v) => v
      case JsNumber(v) if v.isValidDouble => v.toDouble
      case JsNumber(v) if v.isValidFloat => v.toFloat
      case JsNumber(v) if v.isValidLong => v.toLong
      case JsNumber(v) => v.toInt
      case JsString(v) => v
      case JsArray(v) => v.map(toBSON).asJava
      case JsObject(fields) => new BasicBSONObject(fields.map(field => field._1 -> toBSON(field._2)).toMap.asJava)
      case _: JsUndefined => null
    }
  }

  def toJSON(bson: BSONObject) = {
    Json.parse(bson.toString)
  }

}
