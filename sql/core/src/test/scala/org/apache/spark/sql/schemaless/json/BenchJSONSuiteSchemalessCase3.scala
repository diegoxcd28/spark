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

class BenchJSONSuiteSchemalessCase3 extends QueryTest with BeforeAndAfterAll {
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
  test("JSON-reviews4-caching"){
    cacheTable("schemalessGoogleReviews")
    checkAnswer(
      sql("select t.userName, t.rating, t.gPlusPlaceId, t.utime " +
        "from schemalessGoogleReviews  as t " +
        "where t.texttime  = 'Feb 15, 2008' and t.gPlusUserId='102890103945181553910'"),
      Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
    )
  }
  test("JSON-reviews4-cached"){

    checkAnswer(
      sql("select t.userName, t.rating, t.gPlusPlaceId, t.utime " +
        "from schemalessGoogleReviews  as t " +
        "where t.texttime  = 'Feb 15, 2008' and t.gPlusUserId='102890103945181553910'"),
      Seq(Row("Dave Hoekstra",1000.0,"116431132822898702569",1203110458L))
    )
  }
  test("JSON-reviews5-cached"){

    checkAnswer(
      sql("select t.userName, t.rating, t.gPlusPlaceId, t.utime " +
        "from schemalessGoogleReviews t " +
        "where t.texttime  = 'Mar 26, 2003' and t.gPlusUserId='103192135926796949948' "),
      Seq(Row("Tim Shanley",4000.0,"110101154558443724899",1048723200L))
    )
    uncacheTable("schemalessGoogleReviews")
  }


 }
