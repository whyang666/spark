import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._


case class A(name: String, age: Long)

case class Source(key: String, value: Long)

case class QueryResult(query: String, durations: Seq[Long], physicalPlan: Seq[String]) {
  override def toString() = s"""Query:$query \n Duration: ($durations)\n ${physicalPlan.mkString("\n")}"""
}

object T {
  def friend(hc: HiveContext): Unit = {
    val schema = StructType(
      Array(
        StructField("name", StringType, false),
        StructField("friend", StringType, false)
      )
    )
    // A few rows
    val friends = Array(
      Row("Thoralf", "Karim"),
      Row("Larry", "Larry"),
      Row("Karim", "Thoralf")
    )

    val friendsRDD = hc.sparkContext.parallelize(friends)

    // In the first case, the Array type of the field "friends" is predefined in the schema
    val friendsSRDD1 = hc.applySchema(friendsRDD, schema)
    friendsSRDD1.registerTempTable("friends1")

    // In the second case, the Array type of the field "friends" is obtained with a query
    val friendsSRDD2 = hc.sql("SELECT name as n, friend as f FROM friends1")
    friendsSRDD2.registerTempTable("friends2")

    hc.sql("set spark.sql.shuffle.partitions=1")
    val fullJoinFriends1 = hc.sql("SELECT alias1.friend, alias2.friend, alias1.name, alias2.name FROM friends1 as alias1 LEFT JOIN friends1 as alias2 ON (alias1.friend = alias2.name)")
    fullJoinFriends1.collect foreach println

    println("\nThe incorrect result")
    val fullJoinFriends2 = hc.sql("SELECT alias1.f, alias2.f, alias1.n, alias2.n FROM friends2 as alias1 LEFT JOIN friends2 as alias2 ON (alias1.f = alias2.n)")
    fullJoinFriends2.collect foreach println
    println("----")
  }

  def prepare(hc: HiveContext): Unit = {
    val schema = StructType(
      Array(
        StructField("key", IntegerType, false),
        StructField("value", StringType, false)
      )
    )

    val dataRDD = hc.sparkContext.parallelize((1 to 100000).map(i => Row(i % 100, i.toString * 8)))

    // In the first case, the Array type of the field "friends" is predefined in the schema
    val dataRDD1 = hc.applySchema(dataRDD, schema)
    dataRDD1.registerTempTable("data1")

    // In the second case, the Array type of the field "friends" is obtained with a query
    val dataRDD2 = hc.sql("SELECT (key+5) k, value v FROM data1")
    dataRDD2.registerTempTable("data2")

    hc.sql("set spark.sql.shuffle.partitions=1")
    hc.sql("set spark.sql.autoSortMergeJoin=false")
  }

  def run(hc: HiveContext): Unit = {
    val innerJoinData = hc.sql("SELECT * FROM data1 INNER JOIN data2 ON data1.key = data2.k")
    innerJoinData.collect // foreach println
  }

  def aggregate(hc: HiveContext) = {
    //    val constant = 500000
    //    val data = sc.parallelize(1 to 75, 75).mapPartitions { i => {
    //        val rand = new java.util.Random(System.currentTimeMillis())
    //        new Iterator[Source] {
    //          var count = constant
    //          def hasNext = count >= 0
    //          def next(): Source = {
    //            count -= 1
    //            Source("Key" + rand.nextLong(), rand.nextLong())
    //          }
    //        }
    //      }
    //    }
    //    hc.createSchemaRDD(data).saveAsParquetFile("/tmp/source")

    hc.parquetFile("/tmp/source").registerTempTable("source")
    hc.sql("set spark.sql.shuffle.partitions=120")
    // hc.sql("set spark.sql.codegen=true")
    hc.sql("create table IF NOT EXISTS result (c1 double, c2 double, c3 double, c4 double, c5 double)")

    val q1 = "insert overwrite table result select count(value), max(value), min(value), sum(value), avg(value) from source"
    val q2 = "insert overwrite table result select count(value), max(value), min(value), sum(value), avg(value) from source group by key"
    val q3 = "insert overwrite table result select count(distinct value), max(value), min(value), sum(distinct value), avg(value) from source"
    val q4 = "insert overwrite table result select count(distinct value), max(value), min(value), sum(distinct value), avg(value) from source group by key"

    val results = new collection.mutable.ArrayBuffer[QueryResult]()
    try {
      //      (q1 :: q2 :: q3 :: q4 :: Nil).foreach(q => {
      (q3 :: Nil).foreach(q => {
        //      (q3 :: q4 :: Nil).foreach(q => {
        val physicalPlan = hc.sql(s"explain $q").collect().map(_.getString(0))
        val durations = (0 to 5).map { i =>
          val a = System.currentTimeMillis()
          hc.sql(q)
          val b = System.currentTimeMillis()
          b - a
        }git checkout -b
        val qr = QueryResult(q, durations, physicalPlan)
        results.append(qr)
        println(qr)
      })
    } catch {
      case t: Throwable => t.printStackTrace()
    } finally {
      results.foreach(println)
    }
  }

  //  def json(hc: HiveContext): Unit = {
  //    val rdd = hc.sparkContext.parallelize(1 to 1000000).map(i => Source("key" + i, i))
  //    import hc.implicits._
  //    for(i <- 1 to 3) {
  //      val s = System.currentTimeMillis()
  //      println(rdd.toJSON.count)
  //      val e = System.currentTimeMillis()
  //      println(e - s)
  //    }
  //  }

  //  def explain(hc: HiveContext): Unit = {
  //    import org.apache.spark.sql.Dsl._
  //
  //    val df = hc.sql("select key from src where value < 'val_84'").toDataFrame.where('key > 3)
  //
  //    df.explain()
  //    println("---------------")
  //    df.explain(true)
  //  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "TestSQLContext", new SparkConf().set("spark.shuffle.spill", "true"))
    val hc = new HiveContext(sc)

    // build data
    prepare(hc)

    val s = System.currentTimeMillis()
    //aggregate(hc)
    //friend(hc)
    run(hc)
    val e = System.currentTimeMillis()
    println("Time taken: " + (e - s).toString + " ms")
  }
}

