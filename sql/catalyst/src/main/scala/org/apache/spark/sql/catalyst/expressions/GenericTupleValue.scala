package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.TupleValue





/**
 * Created by diegoxcd on 2/24/15.
 */
/**
 * A tuple implementation that uses an map of string -> objects as the underlying storage.  Note that, while
 * the map is not copied, and thus could technically be mutated after creation, this is not
 * allowed.
 */
class GenericTupleValue(protected[sql] val attributes : Map[String, Any]) extends TupleValue {


  def this() = this(Map.empty[String,Any])

  def this(names: Seq[String], values: Seq[Any]) = {
    this(names.zip(values).toMap)
  }





  override def toSeq = attributes.toSeq

  override def toMap = attributes

  override def length = attributes.size

  override def apply(s: String) = attributes.getOrElse(s,null)

  override def isNullAt(s: String) = apply(s) == null



  // Custom hashCode function that matches the efficient code generated version.
  override def hashCode: Int = {
    var result: Int = 37

    var i = 0
    for (attr <- getAttributes()) {
      val update: Int =
        if (isNullAt(attr)) {
          0
        } else {
          apply(attr) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }

  def copy() = this

  /** Returns all the attributes of the tuple */
  override def getAttributes(): Seq[String] = attributes.keys.toSeq
}
