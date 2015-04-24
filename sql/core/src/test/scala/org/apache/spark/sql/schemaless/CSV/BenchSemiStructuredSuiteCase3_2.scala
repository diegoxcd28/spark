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

class BenchSemiStructuredSuiteCase3_2 extends QueryTest with BeforeAndAfterAll {
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



   test("CSV-reviews4-caching"){
     cacheTable("OGoogleReviews")
     checkAnswer(
       sql("select t.userName, rating, t.uTime  " +
         "from OGoogleReviews  t  " +
         "where  t.textTime  = CAST('2008-02-15' as Date)   "),
       Seq(Row("Dave Hoekstra",1000.0,1203110458L),
       Row("Douglas C",1000.0,1203105940L),
       Row("Eric Freeman",4000.0,1203069389L),
       Row("Jim Willson",3000.0,1203111928L),
       Row("Jose Antonio Rodriguez" ,5000.0,1203127483L))
     )
   }
   test("CSV-reviews4-caching2"){

     checkAnswer(
       sql("select t.userName, rating, t.uTime  " +
         "from OGoogleReviews  t  " +
         "where  t.textTime  = CAST('2008-02-15' as Date)   "),
       Seq(Row("Dave Hoekstra",1000.0,1203110458L),
         Row("Douglas C",1000.0,1203105940L),
         Row("Eric Freeman",4000.0,1203069389L),
         Row("Jim Willson",3000.0,1203111928L),
         Row("Jose Antonio Rodriguez" ,5000.0,1203127483L))
     )
     uncacheTable("OGoogleReviews")
   }
   test("CSV-reviews4-caching3"){
     cacheTable("OGoogleReviews2")
     checkAnswer(
       sql("select t.userName, rating " +
         "from OGoogleReviews2  t  " +
         "where  t.textTime  = CAST('2008-02-15' as Date)   "),
       Seq(Row("Dave Hoekstra",1000.0),
         Row("Douglas C",1000.0),
         Row("Eric Freeman",4000.0),
         Row("Jim Willson",3000.0),
         Row("Jose Antonio Rodriguez" ,5000.0))
     )
   }
   test("CSV-reviews4-caching4"){

     checkAnswer(
       sql("select t.userName, rating " +
         "from OGoogleReviews2  t  " +
         "where  t.textTime  = CAST('2008-02-15' as Date)   "),
       Seq(Row("Dave Hoekstra",1000.0),
         Row("Douglas C",1000.0),
         Row("Eric Freeman",4000.0),
         Row("Jim Willson",3000.0),
         Row("Jose Antonio Rodriguez" ,5000.0))
     )
     uncacheTable("OGoogleReviews2")
   }
   /*test("CSV-reviews5-cached"){

     checkAnswer(
       sql("select t.userName, rating, u.userName  " +
         "from GoogleReviews  t join GoogleUsers u on t.userId=u.userId " +
         "where  t.textTime  = CAST('2008-02-15' as Date)  and t.userId='102890103945181553910' "),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }*/



 }
