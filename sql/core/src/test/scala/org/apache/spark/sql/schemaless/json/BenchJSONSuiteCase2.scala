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

import java.util.TimeZone

import org.apache.spark.sql.{QueryTest, Row, SQLConf}
import org.scalatest.BeforeAndAfterAll

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

class BenchJSONSuiteCase2 extends QueryTest with BeforeAndAfterAll {
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



   test("JSON-reviews2-non-cached"){
     checkAnswer(
       sql("select t.userName, t.rating, t.gPlusPlaceId, utime  " +
         "from GoogleReviews  as t " +
         "where t.texttime  = 'Feb 15, 2005'"),
       Seq(Row("Andrew Wolin",4000,"107075848910690400837",1108512000L),
         Row("Andrew Wolin",5000,"106518854042425385717",1108512000L))
     )
   }
   test("JSON-reviews3-non-cached"){

     checkAnswer(
       sql("select userName, rating, gPlusPlaceId, utime " +
         "from GoogleReviews " +
         "where texttime  = 'Sep 23, 2002'"),
       Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
         Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
         Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
   }

   test("JSON-openreviews2-non-cached"){

     checkAnswer(
       sql("select userName, rating, gPlusPlaceId, t.utime   " +
         "from OGoogleReviews t " +
         "where t.texttime  = 'Feb 15, 2005'"),
       Seq(Row("Andrew Wolin",4000,"107075848910690400837",1108512000L),
         Row("Andrew Wolin",5000,"106518854042425385717",1108512000L))
     )
   }
   test("JSON-openreviews3-non-cached"){

     checkAnswer(
       sql("select userName, rating, gPlusPlaceId, t.utime " +
         "from OGoogleReviews t " +
         "where texttime  = 'Sep 23, 2002'"),
     Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
       Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
       Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
   }


   test("JSON-schemaless2-non-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.gPlusPlaceId, t.utime " +
         "from schemalessGoogleReviews as t " +
         "where t.texttime  = 'Feb 15, 2005'"),
       Seq(Row("Andrew Wolin",4000,"107075848910690400837",1108512000L),
         Row("Andrew Wolin",5000,"106518854042425385717",1108512000L))
     )
   }
   test("JSON-schemaless3-non-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.gPlusPlaceId, t.utime  " +
         "from schemalessGoogleReviews as t " +
       "where t.texttime  = 'Sep 23, 2002'"),
     Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
       Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
       Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
   }






 }
