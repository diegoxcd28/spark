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

package org.apache.spark.sql.schemaless

import java.sql.Date

import org.apache.spark.sql.TestData.TestData2
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, TestData}



/* Implicits */
import org.apache.spark.sql.test.TestSQLContext.implicits._

object TestNoSchemaData {

  case class twoData(s: String, i:Int)

  val nullableRepeatedData =
    TestSQLContext.sparkContext.parallelize(
      List.fill(2)(twoData(null,2)) ++
        List.fill(2)(twoData("test",2))++
        List.fill(4)(twoData(null,3)) ++
        List.fill(4)(twoData("test",3)),2).toDF()
  nullableRepeatedData.registerTempTable("nullableRepeatedData")

  //val rdd = TestSQLContext.sparkContext.parallelize(Row(1,Seq("a",2,3),3.4) :: Row(3,Seq(1,2,3),1,2) :: Nil)
  //val schema = StructType(
  //      StructField ("a",IntegerType, true) ::
  //      StructField ("b",ArrayType(IntegerType), true) ::
  //      StructField ("c",DoubleType, true) :: Nil
  //)
  val rowRDD = TestSQLContext.sparkContext.parallelize(
    Row(Map("a" -> 1,"b" -> Map("bb" -> 3),"c" -> 3.4)) ::
      Row(Map("a" -> 3,"b" -> Seq(1,2),"c" -> 1, "d" -> 2)) :: Nil)

  val mix = TestSQLContext.createDataFrame(rowRDD,AnyType)

  mix.registerTempTable("myrows")

  val rowRDD1 = TestSQLContext.sparkContext.parallelize(
    Row(1,1) :: Row(1,3.5) :: Nil)

  val mix1 = TestSQLContext.createDataFrame(rowRDD1,StructType(Seq(StructField("a",IntegerType),StructField("b",DoubleType))))

  mix1.registerTempTable("rows")

  val rowRDD2 = TestSQLContext.sparkContext.parallelize(
    //Row(2,3) :: Row(3,4) :: Nil)
    Row(Map("a" -> 2,"b" -> 3,"c" -> true)) :: Row(Map("a" -> 3,"b" -> 7.0, "c"-> false)) :: Nil)

  val mix2 = TestSQLContext.createDataFrame(rowRDD2,AnyType)//StructType(StructField("a",IntegerType)::StructField("b",IntegerType)::Nil))

  mix2.registerTempTable("IntsAsFloats")

  val testD =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 5) ::
        TestData(2, "3.2") ::
        TestData(3, "1") ::
        TestData(3, "2") :: Nil, 2).toDF()
  testD.registerTempTable("testData77")

  case class TestData8(a : Int, b: Float)
  val testD8 =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(2, 5) ::
        TestData2(1, 2) ::
        TestData2(2, 5) ::
        TestData8(2, 3.0.toFloat) ::
        TestData8(3, 3.41.toFloat) ::
        TestData8(3, 3.2.toFloat) :: Nil, 2).toDF()
  testD8.registerTempTable("testDataIntFloat")

  case class TestDataString(a: Int, b: String)

  val testH =
    TestSQLContext.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(2, 5) ::
        TestData2(1, 2) ::
        TestData2(2, 5) ::
        TestData8(2, 3.0.toFloat) ::
        TestData8(3, 3.41.toFloat) ::
        TestData8(3, 3.2.toFloat) ::
        TestDataString(1,"a" ) ::
        TestDataString(4,"x") ::
        TestDataString(1,"a") ::
        TestDataString(2, "b") ::
        Nil, 2).toDF()
  testH.registerTempTable("testHeterogeneousData")

  val rowSchemaless = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => Row(Map ("key" -> i,"value" -> i.toString ))))
  val testDataSchemaless = TestSQLContext.createDataFrame(rowSchemaless,AnyType)
  testDataSchemaless.registerTempTable("testDataSchemaless")





  val openRDD = TestSQLContext.sparkContext.parallelize((
    Row(1, 2.3, List(1), List(3,4), Map("a" -> 3.0,"b" -> 7.0, "c"-> "option")) :: Nil) ++
    (1 to 100).map(i => Row(1,i.toDouble,i.toString,List(i),null)) ++
    Seq(Row(2, 3.0, "a"    , List(2,3), Map("x" -> 2,"y" -> 3,"z" -> true))) ++
    (1 to 100).map(i => Row(2,i.toDouble,"xx",List(2,i),Map("a"->(i+1), "y" -> (i*2).toDouble))),
    2)

  val sType = OpenStructType(
    StructField("number",IntegerType) ::
    StructField("float",DoubleType) ::
    StructField("any", AnyType) ::
    StructField("array", ArrayType(IntegerType))::Nil)

  val openSchemaRDD = TestSQLContext.createDataFrame(openRDD,sType)

  openSchemaRDD.registerTempTable("openTable")

  val negativeData2 = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => TestData2(i, (-i)))).toDF()
  negativeData2.registerTempTable("negativeData2")



}
