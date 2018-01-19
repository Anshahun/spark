import org.apache.spark.sql.SparkSession

object DataSourceDemo {
  case class Record(key: Int, value: String)

  //用dataSet时后要导入隐式转换
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().appName("app").getOrCreate()
    //val userDF = spark.read.format("json").option("multiLine",true).load("C:\\Users\\Administrator\\Desktop\\aa.json")//读取多行json设置multiLine=true
    //userDF.show()
    //demo4(spark)
    //spark.sql("select * from people_bucketed").show()
    //demo5(spark)
  }

  def demo(spark:SparkSession) ={
    val dataFrame = spark.read.format("json").load("C:\\Users\\Administrator\\Desktop\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    //dataFrame.write.format("parquet").save("user")
    dataFrame.write.bucketBy(42,"name").sortBy("age").saveAsTable("people_bucketed")
    spark.sql("select * from people_bucketed").show()
  }

  def demo2(spark:SparkSession) ={
    val dataFrame = spark.read.format("parquet").load("H:\\idea\\spark2\\user")
    dataFrame.createOrReplaceTempView("user")
    val dataSet = spark.sql("select * from user")
    dataSet.show()
    //dataFrame.write.format("parquet").save("user")
  }

  def demo3(spark:SparkSession) ={
    val dataFrame = spark.sql("select * from json.`C:\\Users\\Administrator\\Desktop\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json`")
    dataFrame.show()
  }

  //partition会新建文件夹，bucket不会？
  def demo4(spark:SparkSession): Unit ={
    val dataFrame = spark.read.format("json").load("C:\\Users\\Administrator\\Desktop\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    dataFrame.write.partitionBy("age").bucketBy(42, "name").option("path","temp").saveAsTable("people_partitioned_bucketed")
  }

  def demo5(spark:SparkSession): Unit ={
    //import spark.implicits._

    val peopleDF = spark.read.json("C:\\Users\\Administrator\\Desktop\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0))
    //namesDF.filter(row=>row(1)=="")
  }

  def demo6(spark:SparkSession): Unit ={
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
  }

}
