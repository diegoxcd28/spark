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

class BenchJSONSuiteSchemalessCase1 extends QueryTest with BeforeAndAfterAll {
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


   test("JSON-reviews0-caching"){
     cacheTable("schemalessGoogleReviews")
     checkAnswer(
       sql("select t.userName, t.rating, t.gPlusPlaceId " +
         "from schemalessGoogleReviews t " +
         "where t.gPlusPlaceId = '101447826328260261560'"),
       Seq(Row("Ben L",5000.0,"101447826328260261560"),
         //Row("Bob Pacheco",5000,"101447826328260261560"),
         Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
         //Row("Chris Hoyt",5000,"101447826328260261560"),
         //Row("Cory B",2000,"101447826328260261560"),
         //Row("Jake Oldroyd",5000,"101447826328260261560"),
         //Row("konomi tetsugawa",1000,"101447826328260261560"),
         Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
         Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
         Row("Mandy Brown",5000.0,"101447826328260261560"))
     )
   }
  test("JSON-reviews0-cached"){
    checkAnswer(
      sql("select t.userName, t.rating, t.gPlusPlaceId " +
        "from schemalessGoogleReviews t " +
        "where t.gPlusPlaceId = '101447826328260261560'"),
      Seq(Row("Ben L",5000.0,"101447826328260261560"),
        //Row("Bob Pacheco",5000,"101447826328260261560"),
        Row("COURTNEY SCHOLARI",4000.0,"101447826328260261560"),
        //Row("Chris Hoyt",5000,"101447826328260261560"),
        //Row("Cory B",2000,"101447826328260261560"),
        //Row("Jake Oldroyd",5000,"101447826328260261560"),
        //Row("konomi tetsugawa",1000,"101447826328260261560"),
        Row("Hamid Ismatullayev",5000.0,"101447826328260261560"),
        Row("Jeremiah Jordan",5000.0,"101447826328260261560"),
        Row("Mandy Brown",5000.0,"101447826328260261560"))
    )

  }

   test("JSON-reviews1-cached"){

     checkAnswer(
       sql("select t.userName, t.rating, t.gPlusPlaceId " +
         "from schemalessGoogleReviews t " +
         "where t.gPlusPlaceId = '115163135004337479044' " +
         "and t.rating < 5000 "),
       Seq(Row("Mavron T",2000,"115163135004337479044")//,
         //Row("Mike Smith",1000,"115163135004337479044"),
         //Row("Steve F" ,1000,"115163135004337479044")
       )
     )
     uncacheTable("schemalessGoogleReviews")
   }



 }
