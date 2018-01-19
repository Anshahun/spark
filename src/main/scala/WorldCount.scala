import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object WorldCount {

  def main(args: Array[String]): Unit = {
   /* val spark = SparkSession.builder().appName("app").getOrCreate()
    val logdata = spark.read.textFile("C:\\Users\\Administrator\\Desktop\\word.txt")
    val word = logdata.flatMap(line=>line.split(" ")).map(word=>(word,1))
   // word.map(word=>word->1).reduceByKey(_+_)*/
    println(getNowDateIncloudmm())
    println(getFiveTime("2018/01/18-10:47:40"))

    val list = 1::2::3::Nil
    val l2 = list.filter(_==1)//保留结果为true的
    println(l2)

    val data = "2017/06/04 23:55:02|117.169.4.130|100.76.76.6|117.169.4.130|GET|HTTP/1.1|img4.kwcdn.kuwo.cn|/star/userpl2015/75/27/1496321762915_10786275b.jpg|Mozilla/5.0 (Linux; U; Android 4.3; zh-cn; ZTE Q505T Build/JLS36C) AppleWebKit/537.36 (KHTML, like Gecko)Version/4.0 Chrome/37.0.0.0 MQQBrowser/7.5 Mobile Safari/537.36|http://m.kuwo.cn/?mid=MUSIC_952589&from=sogou|image/jpeg|200|HIT|6510|68278|2017/06/04 23:55:02.000|2017/06/04 23: 55:02.000|2017/06/04 23:55:02.230|NULL|0|117.169.4.130|NULL|NULL|NULL|NULL|NULL|NULL|NULL|0|s-maxage=15552000|0|68278|1|NULL|0|NULL|NULL|NULL 2017/06/04 23:55:02|117.169.4.130|39.160.158.4|117.169.4.130|GET|HTTP/1.1|dh3.kimg.cn|/static/images/public/photo/2014/151/0924/078e2b7b1339ab 6fd643f9a538e52aae.png.small.jpg|Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Mobile/11D25 7 wnl 4.3.2|http://img.m.duba.com/index.html|image/jpeg|200|HIT|6510|9942|2017/06/04 23:55:02.000|2017/06/04 23:55:02.000|2017/06/04 23:55:02. 319|NULL|0|117.169.4.130|NULL|NULL|NULL|NULL|NULL|NULL|NULL|0|cache|86400|9942|1|NULL|0|NULL|NULL|NULL"
    val sp = data.split("\\|")
    for (i<-0 until sp.length)
    println(sp(i))
    println("===================================")
    println(sp(11))
  }

  //包含分钟的日期格式
  def getNowDateIncloudmm():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.MINUTE,-5)
    var current = dateFormat.format(cal.getTime())
    //val times: Date = new Date(current)
    //times
    current
  }

  def getFiveTime(date: String): String = {
    val sdate = date.split(":")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm")
    if (sdate.length >= 2) {
      val d = sdate(0).toString()
      val split = d.split("-")

      val minute = sdate(1).toInt / 5f
      val mi = minute match {
        case m if m < 1.0  => ":00"
        case m if m < 2.0  => ":05"
        case m if m < 3.0  => ":10"
        case m if m < 4.0  => ":15"
        case m if m < 5.0  => ":20"
        case m if m < 6.0  => ":25"
        case m if m < 7.0  => ":30"
        case m if m < 8.0  => ":35"
        case m if m < 9.0  => ":40"
        case m if m < 10.0 => ":45"
        case m if m < 11.0 => ":50"
        case m if m < 12.0 => ":55"
        case _             => "unkown"
      }
      //re=DateTime.parse(d.concat(mi),DateTimeFormat.forPattern("yyyy/MM/dd HH:mm"))
      //re.append(d).append(mi)
      //val t = df.parse(d.concat(mi)).getTime
      //val t = df.parse(d.concat(mi)).getTime
      var str=split(0).replaceAll("/","")+" "+split(1)
      println(f"$str====$mi")
      return str+mi
    }else{
      return "unkown"
    }
  }

}
