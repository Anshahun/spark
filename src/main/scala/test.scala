import org.apache.spark.sql.SparkSession

object test{

  def main(args: Array[String]): Unit = {
   /* val run:Runnable = ()=>{
      val file = "C:\\Users\\Administrator\\Desktop\\word.txt"
      val spark = SparkSession.builder.appName("appname").getOrCreate
      val logdata = spark.read.textFile(file).cache
      val num_a = logdata.filter(line=>line.contains('a')).count()
      val num_b = logdata.filter(line=>line.contains('b')).count()
      println(s"a=$num_a b=$num_b")
      Thread.sleep(60000)
    }*/
    val thread = new Thread(()=>println(1))
    thread.start()
    //new Runnable (()=>{})
  }
}