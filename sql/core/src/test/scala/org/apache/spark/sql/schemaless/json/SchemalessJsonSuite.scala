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

package org.apache.spark.sql.schemaless.json

import java.math.BigInteger
import java.util.TimeZone

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.json.TestJsonData._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll


/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

class SchemalessJsonSuite extends QueryTest with BeforeAndAfterAll {
   val originalColumnBatchSize = conf.columnBatchSize
   val originalInMemoryPartitionPruning = conf.inMemoryPartitionPruning
   // Make sure the tables are loaded.
   TestJsonData

   var origZone: TimeZone = _
   override protected def beforeAll() {
     origZone = TimeZone.getDefault
     TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
     // Make a table with 5 partitions, 2 batches per partition, 10 elements per batch
     //setConf(SQLConf.COLUMN_BATCH_SIZE, "10")


     // Enable in-memory partition pruning
     setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, "true")
   }

   override protected def afterAll() {
     TimeZone.setDefault(origZone)

     //setConf(SQLConf.COLUMN_BATCH_SIZE, originalColumnBatchSize.toString)
     setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, originalInMemoryPartitionPruning.toString)
   }


  test("Primitive field and type inferring") {
    val jsonDF = jsonRDD(primitiveFieldAndType)

    val expectedSchema = StructType(
      StructField("bigInteger", DecimalType.Unlimited, true) ::
        StructField("boolean", BooleanType, true) ::
        StructField("double", DoubleType, true) ::
        StructField("integer", LongType, true) ::
        StructField("long", LongType, true) ::
        StructField("null", StringType, true) ::
        StructField("string", StringType, true) :: Nil)

    assert(expectedSchema === jsonDF.schema)

    jsonDF.registerTempTable("jsonTable")


    checkAnswer(
      sql("select * from jsonTable"),
      Row(new java.math.BigDecimal("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string.")
    )
  }
  test("Primitive field no type inference" ) {

    val expectedSchema = StructType(
        StructField("bigInteger", DecimalType.Unlimited, true) ::
        StructField("boolean2", BooleanType, true) ::
        StructField("double", DoubleType, true) ::
        StructField("integer", LongType, true) ::
        StructField("long", LongType, true) ::
        StructField("null", StringType, true) ::
        StructField("string", StringType, true) :: Nil)

    val jsonDF = jsonRDD(primitiveFieldAndType, expectedSchema)


    jsonDF.registerTempTable("jsonTable")


    checkAnswer(
      sql("select * from jsonTable"),
      Row(new java.math.BigDecimal("92233720368547758070"),
        null,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string."
      )
    )
  }
  test("Primitive field no type inference - Open Type" ) {


    val expectedSchema = OpenStructType(
      StructField("bigInteger", DecimalType.Unlimited, true) ::
        StructField("boolean2", BooleanType, true) ::
        StructField("double", DoubleType, true) ::
        StructField("integer", LongType, true) ::
        StructField("long", LongType, true) ::
        StructField("null", StringType, true) ::
        StructField("string", StringType, true) :: Nil)

    val jsonDF = jsonRDD(primitiveFieldAndType, expectedSchema)


    jsonDF.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable"),
      Row(new java.math.BigDecimal("92233720368547758070"),
        null,
        1.7976931348623157E308,
        10,
        21474836470L,
        null,
        "this is a simple string.",
        OpenTuple(Map("boolean" ->  true))
      )
    )
  }
  test("Type conflict in complex field values") {


    val expectedSchema = StructType(
      StructField("array", AnyType, true) ::
        StructField("num_struct", AnyType, true) ::
        StructField("str_array", AnyType, true) ::
        StructField("struct", AnyType, true) ::
        StructField("struct_array", AnyType, true) :: Nil)

    val jsonDF = jsonRDD(complexFieldValueTypeConflict,expectedSchema)

    jsonDF.registerTempTable("jsonTable")

    checkAnswer(
      sql("select * from jsonTable "),
      Row(Seq(), 11, Seq(1,2,3),  OpenTuple(),  Seq()) ::
        Row(null, OpenTuple(Map("field" -> false)), null, null, OpenTuple()) ::
        Row(Seq(4, 5, 6), null, "str", OpenTuple(Map("field" -> null)), Seq(7,8,9)) ::
        Row(Seq(7), OpenTuple(),Seq("str1","str2",33), OpenTuple(Map("field" -> "str")),
          OpenTuple(Map("field" -> true))) ::
      Nil
    )
  }
  test("Type conflict in complex field values- no schema") {

    val jsonDF = jsonRDD(complexFieldValueTypeConflict,AnyType)

    jsonDF.registerTempTable("jsonTable")
    checkAnswer(
      sql("select t.array, t.num_struct, t.str_array, t.struct, t.struct_array from jsonTable t "),
      Row(Seq(), 11, Seq(1,2,3),  OpenTuple(),  Seq()) ::
        Row(null, OpenTuple(Map("field" -> false)), null, null, OpenTuple()) ::
        Row(Seq(4, 5, 6), null, "str", OpenTuple(Map("field" -> null)), Seq(7,8,9)) ::
        Row(Seq(7), OpenTuple(),Seq("str1","str2",33), OpenTuple(Map("field" -> "str")),
          OpenTuple(Map("field" -> true))) ::
        Nil
    )
  }

  test("SPARK-2096 Correctly parse dot notations") {
    val schema = StructType(
      StructField("arrayOfArray1", ArrayType(ArrayType(ArrayType(LongType,true),true),true), true) ::
        StructField("arrayOfArray2", ArrayType(ArrayType(ArrayType(AnyType,true),true),true), true) ::
        StructField("arrayOfStruct", ArrayType(AnyType), true) ::
        StructField("complexArrayOfStruct", ArrayType(AnyType), true) ::
         Nil)
    val jsonDF = jsonRDD(complexFieldAndType2,schema)
    jsonDF.registerTempTable("jsonTable")

    checkAnswer(
      sql("select arrayOfStruct[0].field1, arrayOfStruct[0].field2 from jsonTable"),
      Row(true, "str1")
    )
    checkAnswer(
      sql(
        """
          |select complexArrayOfStruct[0].field1[1].inner2[0], complexArrayOfStruct[1].field2[0][1]
          |from jsonTable
        """.stripMargin),
      Row("str2", 6)
    )
  }
  test("SPARK-2096 Correctly parse dot notations - anys") {

    val jsonDF = jsonRDD(complexFieldAndType2,AnyType)
    jsonDF.registerTempTable("jsonTable")

    checkAnswer(
      sql("select t.arrayOfStruct[0].field1, t.arrayOfStruct[0].field2 from jsonTable t"),
      Row(true, "str1")
    )
    checkAnswer(
      sql(
        """
          |select t.complexArrayOfStruct[0].field1[1].inner2[0], t.complexArrayOfStruct[1].field2[0][1]
          |from jsonTable t
        """.stripMargin),
      Row("str2", 6)
    )
  }

  test("Loading a JSON dataset from a text file with SQL") {
    val file = getTempFilePath("json")
    val path = file.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)


    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTableSQL (a int, * )
        |USING org.apache.spark.sql.json
        |OPTIONS (
        |  path '$path'
        |)
      """.stripMargin)

    checkAnswer(
      sql("select t.bigInteger, t.boolean, t.double, t.integer, t.long, t.string from jsonTableSQL t"),
      Row(new BigInteger("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,

        "this is a simple string.")
    )
  }
  test("Loading a JSON dataset from a text file from code - openschema") {
    val file = getTempFilePath("json")
    val path = file.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)


    val schema = OpenStructType(
      StructField("boolean", BooleanType, true) :: Nil)
    val jsonDF = jsonFile(path, schema)
    jsonDF.registerTempTable("jsonTableSQL")


    checkAnswer(
      sql("select t.bigInteger, t.boolean, t.double, t.integer, t.long, t.string from jsonTableSQL t"),
      Row(new BigInteger("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,

        "this is a simple string.")
    )
  }
  test("Loading a JSON dataset from a text file from code - no schema") {
    val file = getTempFilePath("json")
    val path = file.toString
    primitiveFieldAndType.map(record => record.replaceAll("\n", " ")).saveAsTextFile(path)


    val jsonDF = jsonFile(path, AnyType)
    jsonDF.registerTempTable("jsonTableSQL")


    checkAnswer(
      sql("select t.bigInteger, t.boolean, t.double, t.integer, t.long, t.string " +
        "from jsonTableSQL t"),
      Row(new BigInteger("92233720368547758070"),
        true,
        1.7976931348623157E308,
        10,
        21474836470L,

        "this is a simple string.")
    )
  }


  test("Loading a JSON dataset from an existing text file - open type") {


    checkAnswer(
      sql("select t.userName, t.jobs[0][0], t.jobs[0][1], t.currentPlace[1][1], t.currentPlace[1][2] " +
        "from openUsers t where t.userId ='111471133248319526457'"),
      Row("Aleksandra Baruch",
        "Hotel",
        "Recepcjonista",
        532160380,
        169987960
      )
    )
  }

  test("Loading a JSON dataset from an existing text file") {


    checkAnswer(
      sql("select t.userName, t.jobs[0][0], t.jobs[0][1], t.currentPlace[1][1], t.currentPlace[1][2] " +
        "from schemalessUsers t where t.userId ='111471133248319526457'"),
      Row("Aleksandra Baruch",
        "Hotel",
        "Recepcjonista",
        532160380,
        169987960
      )
    )
  }

  test("saving as json") {

    sql("select t.userName, t.jobs[0][0] job1 , t.jobs[0][1] job2, t.currentPlace[1][1] coord_x, t.currentPlace[1][2] coord_y " +
      "from schemalessUsers t " +
      "")
      .save("json", SaveMode.ErrorIfExists, Map("path" -> "i.json"))

  }

  test("saving as unique json file") {

    val data = sql(s"""
    |select t.userName, t.jobs[0][0] work_place , t.jobs[0][1] position,
    |       t.currentPlace[1][1] coord_x, t.currentPlace[1][2] coord_y
    |from schemalessUsers t """.stripMargin).saveAsSingleJSONFile("w.json")

  }


 }
