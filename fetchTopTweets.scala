import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


object fetchTopTweets {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("top_n_tweets").getOrCreate()
    import spark.implicits._
    import spark.sql

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val APIkey = "AnZRnOjPk6sdvxVkcuihKKMcP" // APIkey
    val APIsecretkey = "tnarDIigDW3DIFuzHdfSMrnSQAeSrfgb9ZjcB5HRgpvjTI9QKD" // APIsecretkey
    val Accesstoken = "181460431-6LAFtFJHZFlFdP4zJ5FANcpTtHzv8vx9ZcT4fxqi" // Accesstoken
    val Accesstokensecret = "qrqopeLQLTa5bnpIMcYszlY4tHuBJ1uqbJZaQHsQ4RHZ8" // Accesstokensecret


    val searchFilter = "iPhone" // search query

    val interval = 10



    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)


    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))


    tweetStream.foreachRDD { a =>
      val rdd = a.toString()

      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._


      val df1 = a.map(x => (x.getSource(),x.getRetweetCount() , x.getText())).toDF("createdBy","retweet","text")
      df1.printSchema()
      df1.show(10,false) // n -> Represents the count of top tweets to be fetched. Here n = 10

    }

    ssc.start()
    ssc.awaitTermination()


  }

}