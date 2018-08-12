package com.dt.spark.cores

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 电影点评系统用户行为分析之一
  * 统计观看某部电影的具体用户信息。
  * 实现思想：
  *   1. 通过评分数据过滤出特定电影（1193）的所有用户id
  *   2. 把用户信息中的职业id通过join职业信息数据替换为职业名
  *   3. 把1和2的rdd进行join得到结果数据
  */
object Movie_User_Analyzer_RDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var masterUrl = "local[4]"
    var dataPath = "data/moviedata/medium/"

    if (args.length>0){
      masterUrl = args(0)
    }else if(args.length>1){
      dataPath=args(1)
    }

    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName(this.getClass.getSimpleName))

    val usersRDD = sc.textFile(dataPath+"users.dat")
    val moviesRDD = sc.textFile(dataPath+"movies.dat")
    val occupationsRDD: RDD[String] = sc.textFile(dataPath+"occupations.dat")
    val ratings = sc.textFile(dataPath+"ratings.dat")
    val ratingsRDD: RDD[String] = sc.textFile("data/moviedata/big/ratings.dat")

    val usersBasic: RDD[(String, (String, String, String))] = usersRDD.map(_.split("::")).map { userArr =>
      (//UserID::Gender::Age::Occupation::Zip-code
        userArr(3), (userArr(0), userArr(1), userArr(2))
      )
    }
    for (elem<-usersBasic.collect().take(2)){
      println("userBasicRDD(职业ID，（用户ID，性别，年龄）)："+elem)
    }

    val occupations: RDD[(String, String)] = occupationsRDD.map(_.split("::")).map(job=>(job(0),job(1)))
    for (elem<-occupations.collect().take(2)){
      println("occuption:(职业ID,职业名称)"+elem)
    }

    /**
      * 聚合职业名称到用户信息
      */
    val userInfomation: RDD[(String, ((String, String, String), String))] = usersBasic.join(occupations)
    userInfomation.cache()//存储到缓存
    for (elem <- userInfomation.collect().take(3)){
      println("userInfomation(职业id，（（用户id，性别，年龄），职业名称）)"+elem)
    }

    /**
      * 过滤出对电影ID为1193的电影评过分的用户
      */
    val targetMovie: RDD[(String, String)] = ratingsRDD.map(_.split("::")).map(rating => {
      //UserID::MovieID::Rating::Timestamp
      (rating(0), rating(1))
    }).filter(_._2.equals("1193"))

    for(elem<-targetMovie.collect().take(5)){
      println("targetMovie(用户ID，电影ID)"+ elem)
    }

    //把用户信息转为用户id为key的tuple
    val targetUser: RDD[(String, ((String, String, String), String))] = userInfomation.map(x=>(x._2._1._1,x._2))
    for(elem <- targetUser.collect().take(5)){
      println("targetUser(用户ID，（（用户id，性别，年龄），职业名称）))"+elem)
    }

    println("-----电影点评系统用户行为分析，统计观看电影ID为1193的电影用户信息：用户的ID、性别、年龄、职业名")

    //以用户id作为key，对两组信息进行join，找出观看过电影1193的用户
    val userInfomationForSpecificMovie: RDD[(String, (String, ((String, String, String), String)))] = targetMovie.join(targetUser)


    for(elem<-userInfomationForSpecificMovie){
      println("userInformationForSpecificMovie(用户ID（电影ID，（（用户ID，性别，年龄），职业名称））)"+elem)
    }
    //整理一下信息
    val movieId2userInfo: RDD[(String, (String, String, String, String))] = userInfomationForSpecificMovie.map{case(userid, (movieid, ((userid2, gender, age), occupation)))=>{(movieid,(userid,gender,age,occupation))}}
    for (elem<-movieId2userInfo){
      println("movieId2userInfo(电影id,(用户id，性别，年龄，职业))"+elem)
    }


  }

}
