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

package org.apache.spark.sql

import java.sql.{Timestamp, Date}
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.Interval

class DateFunctionsSuite extends QueryTest {
  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext

  import ctx.implicits._

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

  test("timestamp comparison with date strings") {
    val df = Seq(
      (1, Timestamp.valueOf("2015-01-01 00:00:00")),
      (2, Timestamp.valueOf("2014-01-01 00:00:00"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Timestamp.valueOf("2014-01-01 00:00:00")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2014-06-01"),
      Row(Timestamp.valueOf("2015-01-01 00:00:00")) :: Nil)
  }

  test("date comparison with date strings") {
    val df = Seq(
      (1, Date.valueOf("2015-01-01")),
      (2, Date.valueOf("2014-01-01"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Date.valueOf("2014-01-01")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2015"),
      Row(Date.valueOf("2015-01-01")) :: Nil)
  }

  test("date format") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(date_format("a", "y"), date_format("b", "y"), date_format("c", "y")),
      Row("2015", "2015", "2013"))

    checkAnswer(
      df.selectExpr("date_format(a, 'y')", "date_format(b, 'y')", "date_format(c, 'y')"),
      Row("2015", "2015", "2013"))
  }

  test("year") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(year("a"), year("b"), year("c")),
      Row(2015, 2015, 2013))

    checkAnswer(
      df.selectExpr("year(a)", "year(b)", "year(c)"),
      Row(2015, 2015, 2013))
  }

  test("quarter") {
    val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(quarter("a"), quarter("b"), quarter("c")),
      Row(2, 2, 4))

    checkAnswer(
      df.selectExpr("quarter(a)", "quarter(b)", "quarter(c)"),
      Row(2, 2, 4))
  }

  test("month") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(month("a"), month("b"), month("c")),
      Row(4, 4, 4))

    checkAnswer(
      df.selectExpr("month(a)", "month(b)", "month(c)"),
      Row(4, 4, 4))
  }

  test("dayofmonth") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dayofmonth("a"), dayofmonth("b"), dayofmonth("c")),
      Row(8, 8, 8))

    checkAnswer(
      df.selectExpr("day(a)", "day(b)", "dayofmonth(c)"),
      Row(8, 8, 8))
  }

  test("dayofyear") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dayofyear("a"), dayofyear("b"), dayofyear("c")),
      Row(98, 98, 98))

    checkAnswer(
      df.selectExpr("dayofyear(a)", "dayofyear(b)", "dayofyear(c)"),
      Row(98, 98, 98))
  }

  test("hour") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(hour("a"), hour("b"), hour("c")),
      Row(0, 13, 13))

    checkAnswer(
      df.selectExpr("hour(a)", "hour(b)", "hour(c)"),
      Row(0, 13, 13))
  }

  test("minute") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(minute("a"), minute("b"), minute("c")),
      Row(0, 10, 10))

    checkAnswer(
      df.selectExpr("minute(a)", "minute(b)", "minute(c)"),
      Row(0, 10, 10))
  }

  test("second") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(second("a"), second("b"), second("c")),
      Row(0, 15, 15))

    checkAnswer(
      df.selectExpr("second(a)", "second(b)", "second(c)"),
      Row(0, 15, 15))
  }

  test("weekofyear") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(weekofyear("a"), weekofyear("b"), weekofyear("c")),
      Row(15, 15, 15))

    checkAnswer(
      df.selectExpr("weekofyear(a)", "weekofyear(b)", "weekofyear(c)"),
      Row(15, 15, 15))
  }

  test("function date_add") {
    val st1 = "2015-06-01 12:34:56"
    val st2 = "2015-06-02 12:34:56"
    val t1 = Timestamp.valueOf(st1)
    val t2 = Timestamp.valueOf(st2)
    val s1 = "2015-06-01"
    val s2 = "2015-06-02"
    val d1 = Date.valueOf(s1)
    val d2 = Date.valueOf(s2)
    val df = Seq((1, t1, d1, s1, st1), (3, t2, d2, s2, st2)).toDF("n", "t", "d", "s", "ss")
    checkAnswer(
      df.select(date_add(col("d"), col("n"))),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
      df.select(date_add(col("t"), col("n"))),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
      df.select(date_add(col("s"), col("n"))),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
      df.select(date_add(col("ss"), col("n"))),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
      df.select(date_add(column("d"), lit(null))).limit(1), Row(null))
    checkAnswer(
      df.select(date_add(column("t"), lit(null))).limit(1), Row(null))

    checkAnswer(df.selectExpr("DATE_ADD(null, n)"), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_ADD(d, n)"""),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-05"))))
  }

  test("function date_sub") {
    val st1 = "2015-06-01 12:34:56"
    val st2 = "2015-06-02 12:34:56"
    val t1 = Timestamp.valueOf(st1)
    val t2 = Timestamp.valueOf(st2)
    val s1 = "2015-06-01"
    val s2 = "2015-06-02"
    val d1 = Date.valueOf(s1)
    val d2 = Date.valueOf(s2)
    val df = Seq((1, t1, d1, s1, st1), (3, t2, d2, s2, st2)).toDF("n", "t", "d", "s", "ss")
    checkAnswer(
      df.select(date_sub(col("d"), col("n"))),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-05-30"))))
    checkAnswer(
      df.select(date_sub(col("t"), col("n"))),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-05-30"))))
    checkAnswer(
      df.select(date_sub(col("s"), col("n"))),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-05-30"))))
    checkAnswer(
      df.select(date_sub(col("ss"), col("n"))),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-05-30"))))
    checkAnswer(
      df.select(date_sub(lit(null), column("n"))).limit(1), Row(null))

    checkAnswer(df.selectExpr("""DATE_SUB(d, null)"""), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_SUB(d, n)"""),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-05-30"))))
  }

  test("time_add") {
    val t1 = Timestamp.valueOf("2015-07-31 23:59:59")
    val t2 = Timestamp.valueOf("2015-12-31 00:00:00")
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-12-31")
    val i = new Interval(2, 2000000L)
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.select(time_add(col("d"), lit(i))),
      Seq(Row(Timestamp.valueOf("2015-09-30 00:00:02")),
        Row(Timestamp.valueOf("2016-02-29 00:00:02"))))
    checkAnswer(
      df.select(time_add(col("t"), lit(i))),
      Seq(Row(Timestamp.valueOf("2015-10-01 00:00:01")),
        Row(Timestamp.valueOf("2016-02-29 00:00:02"))))
    checkAnswer(
      df.select(time_add(col("d"), lit(null))).limit(1), Row(null))
    checkAnswer(
      df.select(time_add(col("t"), lit(null))).limit(1), Row(null))
  }

  test("time_sub") {
    val t1 = Timestamp.valueOf("2015-10-01 00:00:01")
    val t2 = Timestamp.valueOf("2016-02-29 00:00:02")
    val d1 = Date.valueOf("2015-09-30")
    val d2 = Date.valueOf("2016-02-29")
    val i = new Interval(2, 2000000L)
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.select(time_sub(col("d"), lit(i))),
      Seq(Row(Timestamp.valueOf("2015-07-29 23:59:58")),
        Row(Timestamp.valueOf("2015-12-28 23:59:58"))))
    checkAnswer(
      df.select(time_sub(col("t"), lit(i))),
      Seq(Row(Timestamp.valueOf("2015-07-31 23:59:59")),
        Row(Timestamp.valueOf("2015-12-29 00:00:00"))))
    checkAnswer(
      df.select(time_sub(col("d"), lit(null))).limit(1), Row(null))
    checkAnswer(
      df.select(time_sub(col("t"), lit(null))).limit(1), Row(null))
  }

  test("function add_months") {
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-07-31")
    val df = Seq((1, d1), (2, d2)).toDF("n", "d")
    checkAnswer(
      df.select(add_months(col("d"), col("n"))),
      Seq(Row(Date.valueOf("2015-08-31")), Row(Date.valueOf("2015-09-30"))))
    checkAnswer(
      df.selectExpr("add_months(d, n)"),
      Seq(Row(Date.valueOf("2015-08-31")), Row(Date.valueOf("2015-09-30"))))
  }

  test("function months_between") {
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-02-16")
    val t1 = Timestamp.valueOf("2014-09-30 23:30:00")
    val t2 = Timestamp.valueOf("2015-09-16 12:00:00")
    val s1 = "2014-09-15 11:30:00"
    val s2 = "2015-10-01 00:00:00"
    val df = Seq((t1, d1, s1), (t2, d2, s2)).toDF("t", "d", "s")
    checkAnswer(df.select(months_between(col("t"), col("d"))), Seq(Row(-10.0), Row(7.0)))
    checkAnswer(df.selectExpr("months_between(t, s)"), Seq(Row(0.5), Row(-0.5)))
  }
}
