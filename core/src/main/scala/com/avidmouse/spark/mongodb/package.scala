package com.avidmouse.spark

/**
 * @author avidmouse
 * @version 0.1, 14-6-25
 */
package object mongodb {
  val sparkContext = MongoSparkContext.Implicits

  val rdd = MongoSparkRDD.Implicits
}
