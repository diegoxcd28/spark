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

package org.apache.spark.sql.schemaless.CSV

import java.util.TimeZone

import org.apache.spark.sql.{QueryTest, Row, SQLConf}
import org.scalatest.BeforeAndAfterAll

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

class BenchSemiStructuredSuite extends QueryTest with BeforeAndAfterAll {
   val originalColumnBatchSize = conf.columnBatchSize
   val originalInMemoryPartitionPruning = conf.inMemoryPartitionPruning
   // Make sure the tables are loaded.
   TestCSVData

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


    /*
   test("CSV-amazon"){

     checkAnswer(
       sql("select itemID, rating, reviewerID, helpful.nHelpful from amazon where itemID = 'I008262202'"),
       Row("str1", "str2", null)
     )
   }*/
   test("CSV-reviews0-uncached"){
     //cacheTable("GoogleReviews")
     checkAnswer(
       sql("select t.userName, t.rating, t.placeId " +
         "from GoogleReviews  as t " +
         "where t.placeId = '101447826328260261560'"),
       Seq(Row("Ben L",5000.0,"101447826328260261560"),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
         Row("Mandy Brown",5000.0,"101447826328260261560"))
     )
   }
   test("CSV-reviews1-uncached"){

     checkAnswer(
       sql("select userName, rating, placeId " +
         "from GoogleReviews " +
         "where placeId = '115163135004337479044'"),
       Seq(Row("Dmitry Dima",5000.0,"115163135004337479044"),
         Row("George Brown",5000.0,"115163135004337479044"),
         Row("Mavron T",2000.0,"115163135004337479044"))
     )
   }
   test("CSV-reviews0-caching"){
     cacheTable("GoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId " +
         "from GoogleReviews where placeId = '101447826328260261560'"),
       Seq(Row("Ben L",5000.0,"101447826328260261560"),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
         Row("Mandy Brown",5000.0,"101447826328260261560"))
     )
   }
   test("CSV-reviews1-cached"){

     checkAnswer(
       sql("select userName, rating, placeId " +
         "from GoogleReviews where placeId = '115163135004337479044'"),
       Seq(Row("Dmitry Dima",5000.0,"115163135004337479044"),
     Row("George Brown",5000.0,"115163135004337479044"),
     Row("Mavron T",2000.0,"115163135004337479044"))
     )
   }

   test("CSV-openreviews0-uncached"){
     //cacheTable("GoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId " +
         "from OGoogleReviews where placeId = '101447826328260261560'"),
       Seq(Row("Ben L",5000.0,"101447826328260261560"),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
         Row("Mandy Brown",5000.0,"101447826328260261560"))
     )
   }
   test("CSV-openreviews1-uncached"){

     checkAnswer(
       sql("select userName, rating, placeId " +
         "from OGoogleReviews where placeId = '115163135004337479044'"),
       Seq(Row("Dmitry Dima",5000.0,"115163135004337479044"),
         Row("George Brown",5000.0,"115163135004337479044"),
         Row("Mavron T",2000.0,"115163135004337479044"))
     )
   }


   test("CSV-openreviews0-caching"){
     cacheTable("OGoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId " +
         "from OGoogleReviews where placeId = '101447826328260261560'"),
       Seq(Row("Ben L",5000.0,"101447826328260261560"),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
         Row("Mandy Brown",5000.0,"101447826328260261560"))
     )
   }
   test("CSV-openreviews1-cached"){

     checkAnswer(
       sql("select userName, rating, placeId " +
         "from OGoogleReviews where placeId = '115163135004337479044'"),
       Seq(Row("Dmitry Dima",5000.0,"115163135004337479044"),
         Row("George Brown",5000.0,"115163135004337479044"),
         Row("Mavron T",2000.0,"115163135004337479044"))
     )
   }

   test("CSV-schemaless0-uncached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId " +
         "from schemalessGoogleReviews as t where t.placeId = '101447826328260261560'"),
       Seq(Row("Ben L",5000.0,"101447826328260261560"),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
         Row("Mandy Brown",5000.0,"101447826328260261560"))
     )
   }
   test("CSV-schemaless1-uncached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId " +
         "from schemalessGoogleReviews as t where t.placeId = '115163135004337479044'"),
       Seq(Row("Dmitry Dima",5000.0,"115163135004337479044"),
         Row("George Brown",5000.0,"115163135004337479044"),
         Row("Mavron T",2000.0,"115163135004337479044"))
     )
   }

  test("CSV-schemaless0-caching"){
    cacheTable("schemalessGoogleReviews")
    checkAnswer(
      sql("select t.userName, t.rating, t.placeId " +
        "from schemalessGoogleReviews as t where t.placeId = '101447826328260261560'"),
      Seq(Row("Ben L",5000.0,"101447826328260261560"),
        Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
        Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
        Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
        Row("Mandy Brown",5000.0,"101447826328260261560"))
    )
  }
  test("CSV-schemaless1-cached"){

    checkAnswer(
      sql("select t.userName, t.rating, t.placeId " +
        "from schemalessGoogleReviews as t where t.placeId = '115163135004337479044'"),
      Seq(Row("Dmitry Dima",5000.0,"115163135004337479044"),
        Row("George Brown",5000.0,"115163135004337479044"),
        Row("Mavron T",2000.0,"115163135004337479044"))
    )
  }
  //fix when doesn't have t. on schemaless





 }
