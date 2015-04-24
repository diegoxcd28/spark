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

class BenchSemiStructuredSuiteCase2 extends QueryTest with BeforeAndAfterAll {
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
   test("CSV-reviews1-non-cached3"){
     //cacheTable("GoogleReviews")
     checkAnswer(
       sql("select t.textTime, CAST(t.textTime as STRING), count(*) " +
         "from OGoogleReviews  as t " +
         "group by t.textTime " +
         "having count(*)=3"),
       Seq(Row("Ben L",5000.0,"101447826328260261560",1359856964L),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560",1392170264L),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560",1368834540L),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560",1386390216L),
         Row("Mandy Brown",5000.0,"101447826328260261560",1362578479L))
     )
   }
*/
   test("CSV-reviews2-non-cached"){
     //cacheTable("GoogleReviews")
     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, uTime  " +
         "from GoogleReviews  as t " +
         "where t.textTime  = CAST('2008-02-15' as Date)"),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L),
         Row("Douglas C",1000.0,"105984177414743370583",1203105940L),
         Row("Eric Freeman",4000.0,"115226951833971109268",1203069389L),
         Row("Jim Willson",3000.0,"101606401856496560733",1203111928L),
         Row("Jose Antonio Rodriguez",5000.0,"115910038488867662686",1203127483L))
     )
   }
   test("CSV-reviews3-non-cached"){

     checkAnswer(
       sql("select userName, rating, placeId, uTime " +
         "from GoogleReviews " +
         "where textTime  = CAST('2002-09-23' as Date)"),
       Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
         Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
         Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
   }
   test("CSV-reviews2-caching"){
     cacheTable("GoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, uTime " +
         "from GoogleReviews  as t " +
         "where  t.textTime  = CAST('2008-02-15' as Date)"),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L),
         Row("Douglas C",1000.0,"105984177414743370583",1203105940L),
         Row("Eric Freeman",4000.0,"115226951833971109268",1203069389L),
         Row("Jim Willson",3000.0,"101606401856496560733",1203111928L),
         Row("Jose Antonio Rodriguez",5000.0,"115910038488867662686",1203127483L))
     )
   }
   test("CSV-reviews3-cached"){
    // cacheTable("GoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, uTime " +
         "from GoogleReviews " +
         "where textTime  = CAST('2002-09-23' as Date)"),
       Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
         Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
         Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
     uncacheTable("GoogleReviews")
   }

   test("CSV-openreviews2-non-cached"){

     checkAnswer(
       sql("select userName, rating, placeId, t.uTime   " +
         "from OGoogleReviews t " +
         "where t.textTime  = CAST('2008-02-15' as Date)"),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L),
         Row("Douglas C",1000.0,"105984177414743370583",1203105940L),
         Row("Eric Freeman",4000.0,"115226951833971109268",1203069389L),
         Row("Jim Willson",3000.0,"101606401856496560733",1203111928L),
         Row("Jose Antonio Rodriguez",5000.0,"115910038488867662686",1203127483L))
     )
   }
   test("CSV-openreviews3-non-cached"){

     checkAnswer(
       sql("select userName, rating, placeId, t.uTime " +
         "from OGoogleReviews t " +
         "where textTime  = CAST('2002-09-23' as Date)"),
       Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
         Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
         Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
   }


   test("CSV-openreviews2-caching"){
     cacheTable("OGoogleReviews")
     checkAnswer(
       sql("select userName, rating, placeId, t.uTime  " +
         "from OGoogleReviews t " +
         "where t.textTime  = CAST('2008-02-15' as Date)"),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L),
         Row("Douglas C",1000.0,"105984177414743370583",1203105940L),
         Row("Eric Freeman",4000.0,"115226951833971109268",1203069389L),
         Row("Jim Willson",3000.0,"101606401856496560733",1203111928L),
         Row("Jose Antonio Rodriguez",5000.0,"115910038488867662686",1203127483L))
     )
   }
   test("CSV-openreviews3-cached"){
     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime " +
         "from OGoogleReviews t " +
         "where textTime  = CAST('2002-09-23' as Date)"),
       Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
         Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
         Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
     uncacheTable("OGoogleReviews")
   }

   test("CSV-schemaless2-non-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime " +
         "from schemalessGoogleReviews as t " +
         "where t.textTime  = CAST('2008-02-15' as Date) "),
       Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L),
         Row("Douglas C",1000.0,"105984177414743370583",1203105940L),
         Row("Eric Freeman",4000.0,"115226951833971109268",1203069389L),
         Row("Jim Willson",3000.0,"101606401856496560733",1203111928L),
         Row("Jose Antonio Rodriguez",5000.0,"115910038488867662686",1203127483L))
     )
   }
   test("CSV-schemaless3-non-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.placeId, t.uTime  " +
         "from schemalessGoogleReviews as t " +
         "where t.textTime  = CAST('2002-09-23' as Date)"),
         //"LIMIT 3"),
       Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
         Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
         Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
     )
   }
  test("CSV-schemaless2-caching"){
    cacheTable("schemalessGoogleReviews")
    checkAnswer(
      sql("select t.userName, t.rating, t.placeId, t.uTime  " +
        "from schemalessGoogleReviews as t " +
        "where t.textTime  = CAST('2008-02-15' as Date)"),
      Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L),
        Row("Douglas C",1000.0,"105984177414743370583",1203105940L),
        Row("Eric Freeman",4000.0,"115226951833971109268",1203069389L),
        Row("Jim Willson",3000.0,"101606401856496560733",1203111928L),
        Row("Jose Antonio Rodriguez",5000.0,"115910038488867662686",1203127483L))
    )
  }
  test("CSV-schemaless3-cached"){

    checkAnswer(
      sql("select t.userName, t.rating, t.placeId, t.uTime  " +
        "from schemalessGoogleReviews as t " +
        "where t.textTime  = CAST('2002-09-23' as Date)"),
      Seq(Row("Tom Fisher",4000.0,"108641002030984687729",1032825600L),
        Row("Tom Fisher",4000.0,"113545489359398641622",1032825600L),
        Row("Tom Fisher",5000.0,"116355878365816728756",1032825600L))
    )
  }

  //fix when doesn't have t. on schemaless





 }
