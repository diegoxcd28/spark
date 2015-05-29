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

import org.apache.spark.sql.types._
import org.apache.spark.sql.{OpenTuple, QueryTest, Row, TestData}
import org.scalatest.BeforeAndAfterAll

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

class SchemalessSuite extends QueryTest with BeforeAndAfterAll {
  // Make sure the tables are loaded.
  TestNoSchemaData
  TestData

  var origZone: TimeZone = _
  override protected def beforeAll() {
    origZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override protected def afterAll() {
    TimeZone.setDefault(origZone)
  }


  test("schemaless") {
    checkAnswer(
      sql("SELECT * FROM testData77"),
      Seq(Row(OpenTuple(Map("a" -> 1,"b"->1))),
        Row(OpenTuple(Map("a" -> 1,"b"->2))),
        Row(OpenTuple(Map("a" -> 2,"b"->5))),
        Row(OpenTuple(Map("key" -> 2,"value"->"3.2"))),
        Row(OpenTuple(Map("key" -> 3,"value"->"1"))),
        Row(OpenTuple(Map("key" -> 3,"value"->"2")))
      )
    )
  }

  test("schemaless-2") {
    checkAnswer(
      sql("SELECT  t.a,t.b,  t.key, t.value FROM testData77 as t"),
      Seq(Row(1,1,null,null),Row(1,2,null,null),Row(2,5,null,null),
        Row(null,null,2,"3.2"),Row(null,null,3,"1"),Row(null,null,3,"2"))
    )
  }

  test("schemaless-3") {
    checkAnswer(
      sql("SELECT  t.b, t.b.bb FROM myrows as t "),
      Seq(Row(List(1,2),null),
        Row(OpenTuple(Map("bb" -> 3)),3))
    )
  }
  test("schemaless-array-navigation") {
    checkAnswer(
      sql("SELECT  t.b, t.b[1], t.b['bb'] FROM myrows as t "),
      Seq(Row(List(1,2),2,null),
        Row(OpenTuple(Map("bb" -> 3)),null,3))
    )
  }

  test("schemaless-comparison") {
    checkAnswer(
      sql("SELECT  t.key  FROM testDataSchemaless as t "),
      (1 to 100).map(x => Row(x)).toSeq
    )
  }

  test("comparison") {
    checkAnswer(
      sql("SELECT t.key, t.value  FROM testData as t "),
      (1 to 100).map(x => Row(x,x.toString)).toSeq
    )
  }


  test("schemaless-filter") {
    checkAnswer(
      sql("SELECT t.a,  t.b,  t.key, t.value  FROM testData77 as t where t.a = 1 "),
      Seq(Row(1,1,null,null),Row(1,2,null,null))
    )
  }

  test("schemaless-arithmetic-functions") {
    checkAnswer(
      sql("SELECT t.value, t.value - 1 as w, t.b, t.b + 1 FROM testData77 as t "),
      Seq(Row("1",0.0,null,null),
        Row("2",1.0,null,null),
        Row("3.2",2.2,null,null),
        Row(null,null,1,2),
        Row(null,null,2,3),
        Row(null,null,5,6)
      )
    )
  }

  test("schemaless-arithmetic-functions-2") {
    checkAnswer(
      sql("SELECT t.a, t.b, t.a + t.b FROM testDataIntFloat as t " +
        "Where t.a < 3"),
      Seq(Row(1,1,BigDecimal(2.0)),
        Row(1,2,BigDecimal(3.0)),
        Row(2,3.0,BigDecimal(5.0)),
        Row(2,5,BigDecimal(2+5.0)),
        Row(2,5,BigDecimal(2+5.0))
      )
    )
  }

  test("schemaless-arithmetic-functions-div") {
    checkAnswer(
      sql("SELECT t.a, t.b, t.b / t.a FROM IntsAsFloats as t "),
      Seq(Row(2,3,1.5),
        Row(3,7.0,7.0 / 3)
      )
    )
  }
  test("schemaless-arithmetic-functions-3") {
    checkAnswer(
      sql("SELECT t.a, t.b, t.b % t.a FROM IntsAsFloats as t "),
      Seq(Row(2,3,1.0),
        Row(3,7.0,1.0)
      )
    )
  }
  test("schemaless-arithmetic-functions-negate") {
    checkAnswer(
      sql("SELECT t.a, t.b,  - t.b FROM IntsAsFloats as t "),
      Seq(Row(2,3,-3),
        Row(3,7.0,-7.0)
      )
    )
  }
  test("schemaless-arithmetic-functions-sqrt") {
    checkAnswer(
      sql("SELECT t.a, t.b,  sqrt(t.b), abs(- t.a) FROM IntsAsFloats as t "),
      Seq(Row(2,3,math.sqrt(3),2),
        Row(3,7.0,math.sqrt(7.0),3)
      )
    )
  }
  test("schemaless-arithmetic-functions-multiply") {
    checkAnswer(
      sql("SELECT t.a, t.b,  t.a * t.b FROM IntsAsFloats as t "),
      Seq(Row(2,3,6),
        Row(3,7.0,21.0)
      )
    )
  }


  test("schemaless-cast") {
    checkAnswer(
      sql("SELECT t.b, CAST(t.b as String)  FROM IntsAsFloats as t "),
      Seq(Row(3,"3"),
        Row(7.0,"7.0"))
      )

  }
  test("schemaless-case") {
    checkAnswer(
      sql("SELECT CASE WHEN t.a >0 THEN t.a WHEN t.value >='1'  THEN t.value ELSE 3 END  FROM testData77 as t "),
      Seq(Row(1),Row(1),Row(2),Row("1"),Row("2"),Row("3.2"))
    )
  }

  test("schemaless-in") {
    checkAnswer(
      sql("SELECT t.value FROM testData77 as t WHERE t.value in ('1','4','2',4)"),
      Seq(Row("1"),Row("2"))
    )
  }
  test("schemaless-join") {
    checkAnswer(
      sql("SELECT t.a,  t.b,tt.a, tt.b FROM testData77 as t join testDataIntFloat as tt on t.a=tt.a"),
      //sql("SELECT t.key  FROM testData as t join testData2 as n on t.key = n.a "),
      Seq(Row(1,1,1,1),Row(1,1,1,2),Row(1,2,1,1),Row(1,2,1,2),Row(2,5,2,5),Row(2,5,2,5),
        Row(2,5,2,3.0.toFloat))
    )
  }
  test("schemaless-join-same-table") {
    checkAnswer(
      sql("SELECT t.a, tt.value  " +
        "FROM testData77 as t join testData77 as tt on (t.a=tt.a or t.key=tt.key)"),
      Seq(Row(1,null), Row(1,null), Row(1,null),
       Row(1,null),Row(2,null), Row(null,"1"), Row(null,"1"),
       Row(null,"2"),Row(null,"2"),Row(null,"3.2"))
    )
  }
  test("schemaless-left-join") {
    checkAnswer(
      sql("SELECT t.a, t.b, tt.a, tt.b  " +
        "FROM IntsAsFloats as t left join IntsAsFloats as tt on cast(t.a as double)=cast(tt.b as double)"),
      Seq(Row(2,3,null,null), Row(3,7.0,2,3.0))
    )
  }
  test("schemaless-right-join") {
    checkAnswer(
      sql("SELECT t.a, t.b, tt.key, tt.value  " +
        "FROM testData77 as t right join testData77 as tt on cast(t.a as string)=tt.value " +
        "WHERE tt.value is not null"),
      Seq(Row(1,1,3,"1"),
        Row(1,2,3,"1"),
        Row(2,5,3,"2"),
        Row(null, null, 2,"3.2"))
    )
  }
  test("schemaless-union") {
    checkAnswer(
      sql("SELECT t.key, t.value " +
        "FROM testData77 as t "  +
        "UNION " +
        "SELECT tt.a, tt.b, tt.c " +
        "FROM IntsAsFloats as tt"),
      Seq(Row(2,"3.2"),
        Row(2,3),
        Row(3,"1"),
        Row(3,"2"),
        Row(3.0,7.0),
        Row(null,null))
    )
  }
  test("schemaless-union-all") {
    checkAnswer(
      sql("SELECT t.key, t.value, null as c " +
        "FROM testData77 as t " +
        "WHERE t.key is not null "  +
        "UNION ALL " +
        "SELECT tt.a, tt.b, tt.c " +
        "FROM IntsAsFloats as tt " +
        "WHERE tt.a=2"),
      Seq(Row(2,3,true),
        Row(2,"3.2", null),
        Row(3,"1", null),
        Row(3,"2", null))
    )
  }
  test("schemaless-intersect") {
    checkAnswer(
      sql("SELECT t.a, t.b " +
        "FROM testData77 as t "  +
        "INTERSECT " +
        "SELECT tt.a, tt.b " +
        "FROM testDataIntFloat as tt"),
      Seq(Row(1,1),
        Row(1,2),
        Row(2,5))
    )
  }

  test("schemaless-except") {
    checkAnswer(
      sql("SELECT t.a, t.b, t.key, t.value " +
        "FROM testData77 as t "  +
        "EXCEPT " +
        "SELECT tt.a, tt.b, null, null " +
        "FROM testDataIntFloat as tt"),
      Seq(Row(null, null, 2,"3.2"),
        Row(null,null, 3,"1"),
        Row(null, null, 3, "2"))
    )
  }

  test("schemaless-groupby-sum") {
    checkAnswer(
      sql("SELECT t.a, sum(t.b) " +
        "FROM testDataIntFloat as t "  +
        "WHERE t.a < 3 "+
        "GROUP BY t.a"),
      Seq(Row(1,BigDecimal(3.0)),
        Row(2,BigDecimal(13.0)))
    )
  }
  test("schemaless-groupby-avg") {
    checkAnswer(
      sql("SELECT t.a, avg( t.b) " +
        "FROM testHeterogeneousData as t "  +
        "WHERE t.a < 3 "+
        "GROUP BY t.a"),
      Seq(Row(1,BigDecimal(0.75)),
        Row(2,BigDecimal(3.25)))

    )
  }
  test("schemaless-groupby-count") {
    checkAnswer(
      sql("SELECT t.a, count( t.b) " +
        "FROM testDataIntFloat as t "  +
        "GROUP BY t.a " ),
      Seq(Row(1,2),
        Row(2,3),
        Row(3,2))
    )
  }

  test("schemaless-groupby-count-2") {
    checkAnswer(
      sql("SELECT t.a,t.key, count(t.b), count(t.value) " +
        "FROM testData77 as t "  +
        "GROUP BY t.a, t.key"),
      Seq(Row(1,null,2, 0),
        Row(2,null,1, 0),
        Row(null,2, 0,1),
        Row(null,3, 0,2))
    )
  }
  test("schemaless-groupby-count-star") {
    checkAnswer(
      sql("SELECT t.a,t.key, count(*) " +
        "FROM testData77 as t "  +
        "GROUP BY t.a, t.key"),
      Seq(Row(1,null,2),
        Row(2,null,1),
        Row(null,2,1),
        Row(null,3,2))
    )
  }
  test("schemaless-groupby-max-min") {
    checkAnswer(
      sql("SELECT t.a, min(t.b),  max(t.b) " +
        "FROM testHeterogeneousData as t "  +
        "GROUP BY t.a"),
      Seq(Row(1,1,"a"),
        Row(2,3.0.toFloat,"b"),
        Row(3,3.2.toFloat,3.41.toFloat),
        Row(4,"x","x"))
    )
  }
  test("schemaless-groupby-first") {
    checkAnswer(
      sql("SELECT t.a, first(t.b)   " +
        "FROM testHeterogeneousData as t "  +
        "GROUP BY t.a"),
      Seq(Row(1,1),
        Row(2,5),
        Row(3, 3.41.toFloat),
        Row(4,"x"))
    )
  }//returns the last of the entire relation for the last when it shouldn't
  // check the distinct functions
  // test sort with 
  test("schemaless-groupby-count-distinct") {
    checkAnswer(
      sql("SELECT t.a, count(distinct t.b) " +
        "FROM testHeterogeneousData as t "  +
        "GROUP BY t.a"),
      Seq(Row(1,3),
        Row(2,3),
      Row(3,2),
      Row(4,1))
    )
  }

  test("schemaless-groupby-sum-distinct") {
    checkAnswer(
      sql("SELECT t.a, sum(distinct t.b) " +
        "FROM testDataIntFloat as t "  +
        "WHERE t.a < 3 "+
        "GROUP BY t.a"),
      Seq(Row(1,BigDecimal(3.0)),
        Row(2,BigDecimal(8.0)))
    )
  }

  test("schema-order by") {
    checkAnswer(
      sql("SELECT t.a, t.b " +
        "FROM testData2 as t "  +
        "Order BY t.b, t.a desc"),
      Seq(Row(3,1),
        Row(2,1),
        Row(1,1),
        Row(3,2),
        Row(2,2),
        Row(1,2))
    )
  }
  test("schemaless-order by") {
    checkAnswer(
      sql("SELECT t.a, t.b " +
        "FROM testHeterogeneousData as t "  +
        "Order BY t.a, t.b desc"),
      Seq(Row(1,"a"),
        Row(1,"a"),
        Row(1,2),
        Row(1,1),
        Row(2,"b"),
        Row(2,5),
        Row(2,5),
        Row(2,3.0.toFloat),
        Row(3,3.41.toFloat),
        Row(3,3.2.toFloat),
        Row(4,"x")
      )
    )
  }


  test("nulls") {

    checkAnswer(
      sql("SELECT null=null, t.a=t.a FROM nullInts as t "),
      Seq(Row(null, null),Row(null,true),Row(null,true),Row(null,true))

    )

  }
  test("subq") {

    checkAnswer(
      sql("SELECT t.w FROM (SELECT max(x.a) as w FROM nullInts as x ) as t  "),
      Seq(Row(3))


    )

  }

  test("schemaless-distinct") {
    checkAnswer(
      sql("SELECT distinct  t.a, t.b " +
        "FROM testHeterogeneousData as t "  ),
      Seq(Row(1,"a"),
        Row(1,2),
        Row(1,1),
        Row(2,"b"),
        Row(2,5),
        Row(2,3.0.toFloat),
        Row(3,3.41.toFloat),
        Row(3,3.2.toFloat),
        Row(4,"x")
      )
    )
  }
  test("schemaless-array-indexing") {
    checkAnswer(
      sql("SELECT t.array[1], t.array[2] " +
        "FROM openTable as t " +
        "WHERE t.any='a'"  ),
      Seq(Row(3,null)
      )
    )
  }
  test("schemaless-array-indexing-2") {
    checkAnswer(
      sql("SELECT t.array[t.number] " +
        "FROM openTable as t " +
        "WHERE t.float = 2.3" ),
      Seq(Row(4)
      )
    )
  }
  test("schemaless-array-substring") {
    checkAnswer(
      sql("SELECT substring(t.any,2,2) " +
        "FROM openTable as t "  +
        "WHERE t.float = 2.0"  ),
      Seq(Row(""),Row("x")
      )
    )
  }



}
