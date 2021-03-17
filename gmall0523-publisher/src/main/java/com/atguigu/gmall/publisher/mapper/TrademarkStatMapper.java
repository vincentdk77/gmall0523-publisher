package com.atguigu.gmall.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * Author: Felix
 * Date: 2020/11/3
 * Desc:
 */
public interface TrademarkStatMapper {

    List<Map> selectTradeSum(@Param("start_time")String startTime,
                             @Param("end_time") String endTime,
                             @Param("topN") int topN);
}
