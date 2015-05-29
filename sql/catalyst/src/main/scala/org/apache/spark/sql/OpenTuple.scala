package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.GenericOpenTuple

import scala.util.hashing.MurmurHash3
/**
 * Created by diegoxcd on 2/19/15.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


object OpenTuple {
  /**
   * This method can be used to extract fields from a [[OpenTuple]] object in a pattern match. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row.toSeq)

  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(): OpenTuple = new GenericOpenTuple()

  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(m: Map[String,Any]): OpenTuple = new GenericOpenTuple(m)


  /**
   * Merge multiple rows into a single row, one after another.
   */
  def merge(tuples: OpenTuple*): OpenTuple = {
    throw new UnsupportedOperationException
  }
}

/**
 * Represents one row of output from a relational operator.  Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check `isNullAt` before attempting to retrieve a value that might be null.
 *
 * To create a new TupleValue, use [[RowFactory.create()]] in Java or [[Row.apply()]] in Scala.

 *
 * @group row
 */
trait OpenTuple extends Serializable {
  /** Number of elements in the Tuple. */
  def size: Int = length

  /** Number of elements in the Tuple. */
  def length: Int

  /**
   * Returns the value at position i. If the value is null, null is returned. The following
   * is a mapping between Spark SQL types and return types:
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def apply(a: String): Any

  /**
   * Returns the value with attribute a. If the value is null, null is returned. The following
   * is a mapping between Spark SQL types and return types:
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def get(a: String): Any = apply(a)

  /** Checks whether the value with attribute a is null. */
  def isNullAt(a: String): Boolean
  /** Returns all the attributes of the tuple */
  def getAttributes() : Seq[String]


  /**
   * Returns the value at position i.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](i: String): T = apply(i).asInstanceOf[T]

  override def toString(): String = s"{${this.mkString(",")}}"

  /**
   * Make a copy of the current [[OpenTuple]] object.
   */
  def copy(): OpenTuple

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val len = length
    var i = 0
    for (attr <- getAttributes()) {
      if (isNullAt(attr)) { return true }
      i += 1
    }
    false
  }

  override def equals(that: Any): Boolean = that match {
    case null => false
    case that: OpenTuple =>
      if (this.length != that.length) {
        return false
      }
      var i = 0
      val len = this.length
      for (attr <- getAttributes()) {
        if (apply(attr) != that.apply(attr)) {
          return false
        }
      }
      true
    case _ => false
  }

  override def hashCode: Int = {
    // Using Scala's Seq hash code implementation.
    var n = 0
    var h = MurmurHash3.seqSeed
    val len = length
    for (attr <- getAttributes()) {
      h = MurmurHash3.mix(h, apply(attr).##)
      n += 1
    }
    MurmurHash3.finalizeHash(h, n)
  }

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   */
  def toSeq: Seq[(String,Any)]

  def toMap: Map[String, Any]

  /** Displays all elements of this sequence in a string (without a separator). */
  def mkString: String = mkString("","", "")

  /** Displays all elements of this sequence in a string using a separator string. */
  def mkString(sep: String): String = mkString("",sep,"")

  /**
   * Displays all elements of this traversable or iterator in a string using
   * start, end, and separator strings.
   */
  def mkString(start: String, sep: String, end: String): String = {
    toSeq.foldLeft(start)((s,t) =>
      t match {
        case (a,b) => s+  a + " : " + (if (b==null) "null" else b.toString) + sep + " "
      }).dropRight(2) + end

  }
}

