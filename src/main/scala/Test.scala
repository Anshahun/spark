import org.apache.spark.sql.SparkSession

object Test{

  def main(args: Array[String]): Unit = {
      val file = "./word.txt"
      val spark = SparkSession.builder.appName("appname").getOrCreate
      val logdata = spark.read.textFile(file).cache
      val num_a = logdata.filter(line=>line.contains('a')).count()
      val num_b = logdata.filter(line=>line.contains('b')).count()
      println(s"a=$num_a b=$num_b")

  }
}