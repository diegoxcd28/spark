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

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericTupleValue
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._

object TestCSVData {

  val amazon = TestSQLContext.sparkContext.parallelize(
  """{ "itemID" : "I008262202", "rating": 5.0, "helpful": {"nHelpful": 1, "outOf": 1}, "reviewText": "I have gone through many headphones these are fantastic in all respects, Ive used them with my phone, fantastic and for music again fantastic. I love them. Great shipping too,", "reviewerID": "U676540463", "summary": "Love them!", "unixReviewTime": 1393372800, "category": [["Electronics", "Accessories & Supplies", "Audio & Video Accessories", "Headphones"]], "reviewTime": "02 26, 2014"} """ ::
  """{ "itemID" : "I869978482", "rating": 1.0, "helpful": {"nHelpful": 3, "outOf": 4}, "reviewText": "Hard to use the nook when it's swinging around on the button! If you could just lock it! But you cant so it's not worth it! I would look elsewhere!", "reviewerID": "U944253133", "summary": "360 is just a button", "unixReviewTime": 1378166400, "category": [["Electronics", "Computers & Accessories", "Touch Screen Tablet Accessories", "Cases & Sleeves", "Cases"]], "reviewTime": "09 3, 2013"} """ ::
  """ { "itemID" : "I214729036", "rating": 5.0, "helpful": {"nHelpful": 2, "outOf": 2}, "reviewText": "Works as it suppose to.  I have had this for only a week so far but no resetting issues or anything like that that others have noted.  I think if you set it up right which is super easy to do. Call Comcast or your ISP to send the activation and it works fine.  I also like that this modem is smaller than the one I was paying $7 per month.  Its a win win in my eyes.", "reviewerID": "U349231803", "summary": "Better than paying Comcast", "unixReviewTime": 1371686400, "category": [["Electronics", "Computers & Accessories", "Networking Products", "Modems"]], "reviewTime": "06 20, 2013"} """ ::
  """ { "itemID" : "I612023758", "rating": 5.0, "helpful": {"nHelpful": 1, "outOf": 1}, "reviewText": "Lens functions smooth and takes great pictures. It appears to be well made for the price. Though not a pro the pictures I have taken so far look great and am very satisfied.", "reviewerID": "U832529307", "summary": "Olympus Zuiko 70-300 mm f/4.0-5.6 ED Lens.", "unixReviewTime": 1362614400, "category": [["Electronics", "Camera & Photo", "Lenses", "Camera Lenses", "Digital Camera Lenses"]], "reviewTime": "03 7, 2013"} """ ::
  """ { "itemID" : "I446085295", "rating": 5.0, "helpful": {"nHelpful": 3, "outOf": 3}, "reviewText": "Ive had mine for almost a year of hard use on a daily basis for both work and play. My iPad mini has become an indispensable tool. This is a near perfect and high quality device, built like a tank, sexy design, power easily lasts me all day long in spite of intensive image processing use, excellent display even though its not retina, extremely functional, I can easily hold and work with it in one hand. If you want a device that works consistently well all the time, then pay the higher price and get an iPad mini - you get what you pay for and its definitely worth it. The mini has set and maintains the high standard by which other small tablets are measured by. Hold one in your hand, youll see.", "reviewerID": "U266546712", "summary": "S I M P L Y  I N C R E D I B L E", "unixReviewTime": 1377475200, "category": [["Electronics", "Computers & Accessories", "Tablets"]], "reviewTime" : "0826, 2013"} """ :: Nil);


  // Create an RDD of Person objects and register it as a table.
  val amazonRdd = TestSQLContext.sparkContext.textFile("train.csv").map(_.split(",")).map(p =>
    Row(p(0),p(1).toDouble,Row(p(2).toInt,p(3).toInt),p(4),p(5),p(6),p(7).toLong,p(8)))
  val schemaAmazon = StructType(StructField("itemID",StringType)::
    StructField("rating",DoubleType)::
    StructField("helpful",StructType(StructField("nHelpful",IntegerType)::StructField("outOf",IntegerType)::Nil))::
    StructField("reviewText",StringType)::
    StructField("reviewerID",StringType)::
    StructField("summary",StringType)::
    StructField("unixReviewTime",LongType)::
    StructField("reviewTime",StringType)::Nil)

  val amazon2 =  TestSQLContext.createDataFrame(amazonRdd,schemaAmazon)

  amazon2.registerTempTable("amazon")



  val reviewsRdd = TestSQLContext.sparkContext.textFile("reviews.csv").map(_.split(",")).map { p =>
    if (p.size != 8) {
      Row(null, 0.0,null,Array(),null,null,null,0L )
    } else {
      Row(p(0), p(1).toDouble, p(2), Array(p(3).split(";|\\[|\\]")), p(4),
        if (p(5)=="") {
          null
        } else{
          try {
            new Date(new SimpleDateFormat("MMM dd; yyyy").parse(p(5)).getTime)
          }catch{
            case e:NumberFormatException => null
            case e:Exception => null
          }
        } , p(6), p(7).toLong)
    }
  }



  val schemaReviews = StructType(StructField("userName",StringType)::
    StructField("rating",DoubleType)::
    StructField("review",StringType)::
    StructField("categories",ArrayType(StringType))::
    StructField("placeId",StringType)::
    StructField("textTime",DateType)::
    StructField("userId",StringType)::
    StructField("uTime",LongType)::Nil)

  val reviews =  TestSQLContext.createDataFrame(reviewsRdd,schemaReviews)

  reviews.registerTempTable("GoogleReviews")

  val openReviewsRdd = TestSQLContext.sparkContext.textFile("reviews.csv").map(_.split(",")).map { p =>
    if (p.size != 8) {
      Row("", 0.0,"",Array(),null,null,new GenericTupleValue() )
    } else {
      Row(p(0), p(1).toDouble, p(2), Array(p(3).split(";|\\[|\\]")), p(4),
        if (p(5)=="") {
          null
        } else{
          try {
            new Date(new SimpleDateFormat("MMM dd; yyyy").parse(p(5)).getTime)
          }catch{
            case e:NumberFormatException => null
            case e:Exception => null
          }
      },
        new GenericTupleValue( Map( "userId" -> p(6), "uTime" -> p(7).toLong).toMap))
    }
  }

  val openSchemaReviews = OpenStructType(StructField("userName",StringType)::
    StructField("rating",DoubleType)::
    StructField("review",StringType)::
    StructField("categories",ArrayType(StringType))::
    StructField("placeId",StringType)::
    StructField("textTime",DateType)::Nil)

  val openReviews =  TestSQLContext.createDataFrame(openReviewsRdd,openSchemaReviews)

  openReviews.registerTempTable("OGoogleReviews")


  val noSchemaReviewsRdd = TestSQLContext.sparkContext.textFile("reviews.csv").map(_.split(",")).map { p =>
    if (p.size != 8) {
      Row(new GenericTupleValue())
    } else {
      Row(new GenericTupleValue( Map("userName"-> p(0),"rating" -> p(1).toDouble,
          "review" -> p(2),"categories" -> Array(p(3).split(";|\\[|\\]")), "placeId" -> p(4),
          "textTime" -> {
            if (p(5)=="") {
              null
            } else{
              try {
                new Date(new SimpleDateFormat("MMM dd; yyyy").parse(p(5)).getTime)
              }catch{
                case e:NumberFormatException => null
                case e:Exception => null
              }
            }
          }, "userId" -> p(6), "uTime" -> p(7).toLong).toMap))
    }
  }

  val noSchemaReviews =  TestSQLContext.createDataFrame(noSchemaReviewsRdd,AnyType)

  noSchemaReviews.registerTempTable("schemalessGoogleReviews")



  val usersRdd = TestSQLContext.sparkContext.textFile("users.csv").map(_.split(",")).map { p =>
    if (p.size != 6) {
      Row(null, null, null, null, null, null)
    } else {
      Row(p(0), p(1), p(2), p(3), p(4), p(5))
    }
  }



  val schemaUsers = StructType(StructField("userName",StringType)::
    StructField("userId",StringType)::
    StructField("currentPlace",ArrayType(AnyType))::
    StructField("education",ArrayType(AnyType))::
    StructField("jobs",ArrayType(AnyType))::
    StructField("previousPlaces",ArrayType(AnyType))::Nil)

  val users =  TestSQLContext.createDataFrame(usersRdd,schemaUsers)

  users.registerTempTable("GoogleUsers")

  val openReviewsRdd2 = TestSQLContext.sparkContext.textFile("reviews.csv").map(_.split(",")).map { p =>
    if (p.size != 8) {
      Row("", 0.0,"",Array(),null,null,new GenericTupleValue() )
    } else {
      Row(p(0), p(1).toDouble, p(2), Array(p(3).split(";|\\[|\\]")), p(4),
        new GenericTupleValue( Map("textTime" ->{
          if (p(5)=="") {
            null
          } else{
            try {
              new Date(new SimpleDateFormat("MMM dd; yyyy").parse(p(5)).getTime)
            }catch{
              case e:NumberFormatException => null
              case e:Exception => null
            }
          }
        }, "userId" -> p(6), "uTime" -> p(7).toLong).toMap))
    }
  }

  val openSchemaReviews2 = OpenStructType(StructField("userName",StringType)::
    StructField("rating",DoubleType)::
    StructField("review",StringType)::
    StructField("categories",ArrayType(StringType))::
    StructField("placeId",StringType)::Nil)

  val openReviews2 =  TestSQLContext.createDataFrame(openReviewsRdd2,openSchemaReviews2)

  openReviews2.registerTempTable("OGoogleReviews2")
}
