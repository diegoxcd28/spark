package org.apache.spark.sql.schemaless.json

/**
 * Created by diegoxcd on 4/20/15.
 */


import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types._

object TestJsonData {

  val primitiveFieldAndType =
    TestSQLContext.sparkContext.parallelize(
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "bigInteger":92233720368547758070,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }"""  :: Nil)

  val complexFieldAndType2 =
    TestSQLContext.sparkContext.parallelize(
      """{"arrayOfStruct":[{"field1": true, "field2": "str1"}, {"field1": false}, {"field3": null}],
          "complexArrayOfStruct": [
          {
            "field1": [
            {
              "inner1": "str1"
            },
            {
              "inner2": ["str2", "str22"]
            }],
            "field2": [[1, 2], [3, 4]]
          },
          {
            "field1": [
            {
              "inner2": ["str3", "str33"]
            },
            {
              "inner1": "str4"
            }],
            "field2": [[5, 6], [7, 8]]
          }],
          "arrayOfArray1": [
          [
            [5]
          ],
          [
            [6, 7],
            [8]
          ]],
          "arrayOfArray2": [
          [
            [
              {
                "inner1": "str1"
              }
            ]
          ],
          [
            [],
            [
              {"inner2": ["str3", "str33"]},
              {"inner2": ["str4"], "inner1": "str11"}
            ]
          ],
          [
            [
              {"inner3": [[{"inner4": 2}]]}
            ]
          ]]
      }""" :: Nil)




  val usersDF = jsonFile("users.json", AnyType)
  usersDF.registerTempTable("schemalessUsers")

  val schema =  OpenStructType(
    StructField("userName", StringType) ::
      StructField("jobs",ArrayType(ArrayType(StringType)))::
      StructField("currentPlace",AnyType)::
      Nil
  )
  val openUsersDF = jsonFile("users.json", schema)
  openUsersDF.registerTempTable("openUsers")



  val schemaReviews = StructType(
    StructField("userName",StringType)::
    StructField("rating",IntegerType)::
    StructField("reviewText",StringType)::
    StructField("review",StringType)::
    StructField("categories",ArrayType(StringType))::
    StructField("texttime",StringType)::
    StructField("gPlusPlaceId",StringType)::
    StructField("gPlusUserId",StringType)::
    StructField("utime",LongType)::Nil)

  val reviews =  jsonFile("reviews_1.json",schemaReviews)

  reviews.registerTempTable("GoogleReviews")


  val openSchemaReviews = OpenStructType(
    StructField("userName",StringType)::
    StructField("rating",DoubleType)::
    StructField("reviewText",StringType)::
    StructField("review",StringType)::
    StructField("categories",ArrayType(StringType))::
    StructField("gPlusPlaceId",StringType)::
    StructField("texttime",StringType)::Nil)

  val openReviews =  jsonFile("reviews_1.json",openSchemaReviews)

  openReviews.registerTempTable("OGoogleReviews")


  val schemalessReviews =  jsonFile("reviews_1.json",AnyType)

  schemalessReviews.registerTempTable("schemalessGoogleReviews")
}
