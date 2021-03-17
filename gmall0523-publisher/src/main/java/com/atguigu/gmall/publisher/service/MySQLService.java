package com.atguigu.gmall.publisher.service;

import java.util.List;
import java.util.Map;

/**
 * Author: Felix
 * Date: 2020/11/3
 * Desc:
 */
public interface MySQLService {
    List<Map> getTradeAmount(String startTime,String endTime,int topN);
}
