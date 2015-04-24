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

import java.util.TimeZone

import org.apache.spark.sql.{QueryTest, Row, SQLConf}
import org.scalatest.BeforeAndAfterAll

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

class SemiStructuredSuite extends QueryTest with BeforeAndAfterAll {
  val originalColumnBatchSize = conf.columnBatchSize
  val originalInMemoryPartitionPruning = conf.inMemoryPartitionPruning
  // Make sure the tables are loaded.
  TestNoSchemaData


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


  test("SemiStructured") {
    checkAnswer(
      sql("SELECT t.number, t.float, t.array[0] " +
        "FROM openTable as t"),
      Seq(Row(1,2.3,3),
        Row(2,3.0,2)
      )++(1 to 100).map(i => Row(1,i.toDouble,i))++(1 to 100).map(i => Row(2,i.toDouble,2))
    )
  }

  test("SemiStructured-2") {
    checkAnswer(
      sql("SELECT t.number, t.float, t.x, t.c " +
        "FROM openTable as t"),
      Seq(Row(1,2.3,null, "option"),
        Row(2,3.0,2, null)
      )++(1 to 100).map(i => Row(1,i.toDouble,null,null))++(1 to 100).map(i => Row(2,i.toDouble,null,null))
    )
  }

  test("SemiStructured-cached-case1") {

    cacheTable("openTable")

    checkAnswer(
      sql("SELECT t.number, t.float " +
        "FROM openTable as t " +
        "Where t.number = 1 "),
      Seq(Row(1,2.3)
      )++(1 to 100).map(i => Row(1,i.toDouble))
    )
  }

  test("SemiStructured-cached-case2") {

    cacheTable("openTable")

    checkAnswer(
      sql("SELECT t.number, t.float, t.a, t.y " +
        "FROM openTable as t " +
        "Where t.number = 2 "),
      Seq(Row(2,3.0,null,3)
      )++(1 to 100).map(i => Row(2,i.toDouble,i+1,(i*2).toDouble))
    )
  }
  test("SemiStructured-cached-case3") {

    cacheTable("openTable")

    checkAnswer(
      sql("SELECT t.number, t.float, t.a, t.y, t.z " +
        "FROM openTable as t " +
        "Where t.number = 2 and t.y<10"),
      Seq(Row(2,3.0,null,3,true),Row(2,1.0,2,2.0,null),
        Row(2,2.0,3,4.0,null),Row(2,3.0,4,6.0,null),Row(2,4.0,5,8.0,null))
    )
  }




}
