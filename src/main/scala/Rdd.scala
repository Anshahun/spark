import org.apache.spark.{SparkConf, SparkContext}

object Rdd {

  def main(args: Array[String]): Unit = {
    //The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster.
    // To create a SparkContext you first need to build a SparkConf object that contains information about your application.
    val conf = new SparkConf().setAppName("app").setMaster("local")
    //一个jvm实例上只能存在一个sparkContext
    val spark = new SparkContext(conf)
  }

}
