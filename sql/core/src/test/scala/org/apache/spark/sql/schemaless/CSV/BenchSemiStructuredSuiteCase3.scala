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

class BenchSemiStructuredSuiteCase3 extends QueryTest with BeforeAndAfterAll {
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


   //2003-03-10
   //2003-03-26
   //2003-04-01
   test("CSV-reviews4-non-cached"){
     //cacheTable("GoogleReviews")
     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, uTime " +
         "from GoogleReviews  as t " +
         "where t.textTime  = CAST('2008-02-15' as Date) and userId='102890103945181553910'"),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }
   test("CSV-reviews5-non-cached"){

     checkAnswer(
       sql("select userName, rating, placeId, uTime " +
         "from GoogleReviews " +
         "where textTime  = CAST('2003-03-26' as Date) and userId='103192135926796949948' "),
       Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
     )
   }
   test("CSV-reviews4-caching"){
     cacheTable("GoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, uTime " +
         "from GoogleReviews  as t " +
         "where  t.textTime  = CAST('2008-02-15' as Date)  and userId='102890103945181553910' "),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }
   test("CSV-reviews5-cached"){

     checkAnswer(
       sql("select userName, rating, placeId, uTime " +
         "from GoogleReviews t " +
         "where t.textTime  = CAST('2003-03-26' as Date) and t.userId='103192135926796949948' "),
       Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
     )
   }
   //Project [userName#8,rating#9,placeId#12,uTime#15L]
   //Filter ((textTime#13 = 2003-03-26) && (userId#14 = 103192135926796949948))
   //InMemoryColumnarTableScan [placeId#12,userName#8,textTime#13,userId#14,rating#9,uTime#15L], [(textTime#13 = 2003-03-26),(userId#14 = 103192135926796949948)], (InMemoryRelation [userName#8,rating#9,review#10,categories#11,placeId#12,textTime#13,userId#14,uTime#15L], true, 10000, StorageLevel(true, true, false, true, 1), (PhysicalRDD [userName#8,rating#9,review#10,categories#11,placeId#12,textTime#13,userId#14,uTime#15L], MapPartitionsRDD[9] at map at TestCSVData.scala:56), Some(GoogleReviews))


   test("CSV-openreviews4-non-cached"){
     //cacheTable("GoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, t.uTime " +
         "from OGoogleReviews t " +
         "where t.textTime  = CAST('2008-02-15' as Date) and t.userId='102890103945181553910'"),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }
   test("CSV-openreviews5-non-cached"){

     checkAnswer(
       sql("select userName, rating, placeId, t.uTime " +
         "from OGoogleReviews t " +
         "where t.textTime  = CAST('2003-03-26' as Date) and t.userId='103192135926796949948' "),
       Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
     )
   }


   test("CSV-openreviews4-caching"){
     cacheTable("OGoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, t.uTime  " +
         "from OGoogleReviews t " +
         "where t.textTime  = CAST('2008-02-15' as Date)  and t.userId='102890103945181553910' "),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }
   test("CSV-openreviews5-cached"){
     cacheTable("OGoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, t.uTime  " +
         "from OGoogleReviews t " +
         "where t.textTime  = CAST('2003-03-26' as Date) and t.userId='103192135926796949948' "),
       Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
     )
   }
   //Project [userName#16,rating#17,placeId#20,uTime#62]
   //Filter ((textTime#21 = 2003-03-26) && (userId#61 = 103192135926796949948))
   //Navigate [OGoogleReviews#23.userId AS  userId#61,OGoogleReviews#23.uTime AS  uTime#62]
   //InMemoryColumnarTableScan [userName#16,rating#17,OGoogleReviews#23,textTime#21,placeId#20], [(textTime#21 = 2003-03-26)], (InMemoryRelation [userName#16,rating#17,review#18,categories#19,placeId#20,textTime#21,OGoogleReviews#23], true, 10000, StorageLevel(true, true, false, true, 1), (PhysicalRDD [userName#16,rating#17,review#18,categories#19,placeId#20,textTime#21,OGoogleReviews#23], MapPartitionsRDD[15] at mapPartitions at ExistingRDD.scala:86), Some(OGoogleReviews))


   test("CSV-schemaless4-non-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime  " +
         "from schemalessGoogleReviews as t " +
         "where t.textTime  = CAST('2008-02-15' as Date)  and t.userId='102890103945181553910' "),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }
   test("CSV-schemaless5-non-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime  " +
         "from schemalessGoogleReviews as t " +
         "where t.textTime  = CAST('2003-03-26' as Date) and t.userId='103192135926796949948' "),
       Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
     )
   }
   test("CSV-schemaless4-caching"){
     cacheTable("schemalessGoogleReviews")
     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime  " +
         "from schemalessGoogleReviews as t " +
         "where t.textTime  = CAST('2008-02-15' as Date)  and t.userId='102890103945181553910' "),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
     )
   }
   test("CSV-schemaless5-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime  " +
         "from schemalessGoogleReviews as t " +
         "where t.textTime  = CAST('2003-03-26' as Date) and t.userId='103192135926796949948' "),
       Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
     )
   }

  //fix when doesn't have t. on schemaless





 }
