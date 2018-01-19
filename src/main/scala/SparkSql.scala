import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSql {
  case class Person(name:String,age:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("app").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val frame = spark.read.json("C:\\Users\\Administrator\\Desktop\\test.json")//json必须保持在一行
    frame.show()
    frame.printSchema()
    frame.select("action").show()
    frame.select($"action",$"msg").show()
    frame.filter($"action">2).show()
    // Register the DataFrame as a SQL temporary view
    frame.createOrReplaceTempView("cname")
    val res = spark.sql("select * from cname")
    res.show()
    frame.createGlobalTempView("cname")//创建一个全局范围的视图，他是session共享的
    spark.sql("select * from global_temp.cname").show()

    val caseClassDS = Seq(Person("Andy",123)).toDS()
    caseClassDS.map(person=>person.age+1).show()
   // caseClassDS.show()
    // Seq(Person("Andy", 32)).toDS()

    //返回的是dataSet
    val logdata = spark.read.textFile("C:\\Users\\Administrator\\Desktop\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
    //spark.sparkContext.textFile("") //返回的是rdd
    val peopleDf = logdata.map(line=>line.split(",")).map(attr=>Person(attr(0),attr(1).trim.toInt)).toDF()
    peopleDf.show()
    println("========================")
    peopleDf.printSchema()
    peopleDf.createOrReplaceTempView("people")
    val res2 = spark.sql("select * from people where age between 10 and 20")
    res2.show()
    res2.map(row=>"name:"+row(0)+" age:"+row(1)).show()//可以通过下标的方式操作返回的结果集

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]//指定一个编码器
    val maps = res2.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    println(maps.mkString(","))

    logdata.map(line=>line.split(",")).show()//不指定一个case无法生成row

    runInferSchemaExample(spark)
  }

  def runInferSchemaExample(spark:SparkSession): Unit ={
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(filedName=>StructField(filedName,StringType))
    val schema = StructType(fields)
    val rowRdd = rdd.map(_.split(",")).map(attr=>Row(attr(0),attr(1)))
    val df = spark.createDataFrame(rowRdd,schema)//schema与row的数量要对应
    df.createOrReplaceTempView("people")
    val frame = spark.sql("select * from people")
    frame.show()
    frame.map(row=>"name:"+row(0)).show()
    //println(array)
  }

}
