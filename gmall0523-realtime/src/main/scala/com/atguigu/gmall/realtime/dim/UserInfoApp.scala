package com.atguigu.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, streaming}

/**
  * Author: Felix
  * Date: 2020/10/28
  * Desc: 从Kafka中读取数据，保存到Phoenix
  */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.1基本环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("userInfoApp")
    val ssc = new StreamingContext(conf, streaming.Seconds(5))
    val topic = "ods_user_info"
    val groupId = "user_info_group"
    //1.2获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    var offsetDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size != 0) {
      offsetDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      offsetDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //1.3获取本批次消费数据的偏移量情况
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetRangeDSteram: DStream[ConsumerRecord[String, String]] = offsetDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //1.4对从Kafka中读取的数据进行结构的转换  record(kv)==>UserInfo
    val userInfoDStream: DStream[UserInfo] = offsetRangeDSteram.map {
      record => {
        val jsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])


        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val  betweenMs = curTs - date.getTime
        val age = betweenMs/1000L/60L/60L/24L/365L
        if(age<20){
          userInfo.age_group="20岁及以下"
        }else if(age>30){
          userInfo.age_group="30岁以上"
        }else{
          userInfo.age_group="21岁到30岁"
        }

        if(userInfo.gender=="M"){
          userInfo.gender_name="男"
        }else{
          userInfo.gender_name="女"
        }
        userInfo
      }
    }

    //1.5保存到Phoenix中
    userInfoDStream.foreachRDD{
      rdd=>{
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix(
          "GMALL0523_USER_INFO",
          Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
          new Configuration,
          Some("hadoop202,hadoop203,hadoop204:2181")
        )
        //1.6提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
