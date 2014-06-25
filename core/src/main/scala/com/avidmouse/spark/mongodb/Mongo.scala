package com.avidmouse.spark.mongodb

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
case class Mongo(uris: String) {
  def collection(collection: String) = Mongo.Collection(this, collection)
}

object Mongo {

  import play.api.libs.json._

  case class Collection(db: Mongo, collection: String) {
    def outputUri = db.uris.split("\\?") match {
      case Array(u) => s"$u.$collection"
      case Array(u, options) => s"$u.$collection?$options"
    }

    def find(selector: JsObject) = Collection.QueryBuilder(this, Some(selector))

    def find(selector: JsObject, projection: JsObject) = Collection.QueryBuilder(this, Some(selector), Some(projection))
  }

  object Collection {

    case class QueryBuilder(collection: Collection,
                            queryOpt: Option[JsObject] = None,
                            projectionOpt: Option[JsObject] = None,
                            sortOpt: Option[JsObject] = None,
                            limitOpt: Option[Int] = None,
                            skipOpt: Option[Int] = None) {

      def query(selector: JsObject) = {
        copy(queryOpt = Some(selector))
      }

      def projection(p: JsObject) = {
        copy(projectionOpt = Some(p))
      }

      def sort(s: JsObject) = {
        copy(sortOpt = Some(s))
      }

      def limit(l: Int) = {
        copy(limitOpt = Some(l))
      }

      def skip(s: Int) = {
        copy(skipOpt = Some(s))
      }
    }

  }


}
