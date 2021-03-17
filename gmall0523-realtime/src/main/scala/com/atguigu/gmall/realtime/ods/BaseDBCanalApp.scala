package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author: Felix
  * Date: 2020/10/26
  * Desc: 从Kafka中读取数据，根据表名进行分流处理（canal）
  *
➢ 样例 SQL
INSERT INTO z_user_info VALUES(16,'zhang3','13810001010'),(17,'zhang3','13810001010');
➢ canal 接收到的数据格式
{
"data":[{"id":"16","user_name":"zhang3","tel":"13810001010"},{"id":"17","user_name":"zhang3","tel":"13810001010"}],
"database":"gmall-2020-04","es":1589196502000,"id":4,"isDdl":false,
"mysqlType":{"id":"bigint(20)","user_name":"varchar(20)","tel":"varchar(20)"},
"old":null,"pkNames":["id"],"sql":"","sqlType":{"id":-5,"user_name":12,"tel":12},
"table":"z_user_info","ts":1589196502433,"type":"INSERT"
}
  */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = "gmall0523_db_c"
    var groupId = "base_db_canal_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size > 0){
      //从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      //从最新的位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前批次读取的Kafka主题中偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对接收到的数据进行结构的转换，ConsumerRecord[String,String(jsonStr)]====>jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        //获取json格式的字符串
        val jsonStr: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    //分流：根据不同的表名，将数据发送到不同的kafka主题中去
    jsonObjDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          jsonObj=>{
            //获取操作类型
            val opType: String = jsonObj.getString("type")
            if("INSERT".equals(opType)){
              //获取表名
              val tableName: String = jsonObj.getString("table")
              //获取操作数据
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              //拼接目标topic名称
              var sendTopic= "ods_" + tableName
              //对data数组进行遍历
              import scala.collection.JavaConverters._
              for (dataJson <- dataArr.asScala) {
                //根据表名将数据发送到不同的主题中去
                MyKafkaSink.send(sendTopic,dataJson.toString)
              }
            }
          }
        }
        //提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
