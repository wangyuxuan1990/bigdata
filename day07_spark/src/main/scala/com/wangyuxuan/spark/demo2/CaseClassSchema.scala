package com.wangyuxuan.spark.demo2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

/**
 * @author wangyuxuan
 * @date 2020/1/20 17:00
 * @description 利用反射机制实现把rdd转成dataFrame
 */
//4、定义一个样例类
case class Person(id: String, name: String, age: Int)

object CaseClassSchema {
  def main(args: Array[String]): Unit = {
    // 1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()

    // 2、获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    // 3、读取文件数据
    val data: RDD[Array[String]] = sc.textFile("person.txt").map(_.split(" "))

    // 5、将rdd与样例类进行关联
    val personRDD: RDD[Person] = data.map(x => Person(x(0), x(1), x(2).toInt))

    // 6、将rdd转换成dataFrame
    // 需要手动导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()

    // 7、对dataFrame进行相应的语法操作
    // todo：----------------- DSL风格语法-----------------start
    // 打印schema
    personDF.printSchema()
    // 展示数据
    personDF.show()

    // 获取第一行数据
    val first: Row = personDF.first()
    println("first:" + first)
    // 取出前3位数据
    val top3: Array[Row] = personDF.head(3)
    top3.foreach(println)
    // 获取name字段
    personDF.select("name").show()
    personDF.select($"name").show()
    personDF.select(new Column("name")).show()
    personDF.select("name", "age").show()
    // 实现age +1
    personDF.select($"name", $"age", $"age" + 1).show()
    // 按照age过滤
    personDF.filter($"age" > 30).show()
    val count: Long = personDF.filter($"age" > 30).count()
    println("count:" + count)
    // 分组
    personDF.groupBy($"age").count().show()

    personDF.show()
    personDF.foreach(row => println(row))

    // 使用foreach获取每一个row对象中的name字段
    personDF.foreach(row => println(row.getAs[String]("name")))
    personDF.foreach(row => println(row.get(1)))
    personDF.foreach(row => println(row.getString(1)))
    personDF.foreach(row => println(row.getAs[String](1)))
    // todo：----------------- DSL风格语法--------------------end

    // todo：----------------- SQL风格语法-----------------start
    personDF.createTempView("person")
    // 使用SparkSession调用sql方法统计查询
    spark.sql("select * from person").show
    spark.sql("select name from person").show
    spark.sql("select name,age from person").show
    spark.sql("select * from person where age >30").show
    spark.sql("select count(*) from person where age >30").show
    spark.sql("select age,count(*) from person group by age").show
    spark.sql("select age,count(*) as count from person group by age").show
    spark.sql("select * from person order by age desc").show
    // todo：----------------- SQL风格语法----------------------end

    // 关闭sparkSession对象
    spark.stop()
  }
}
