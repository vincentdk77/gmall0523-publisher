package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.TrademarkStatMapper;
import com.atguigu.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Author: Felix
 * Date: 2020/11/3
 * Desc:
 */
@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTradeAmount(String startTime, String endTime, int topN) {
        return trademarkStatMapper.selectTradeSum(startTime,endTime,topN);
    }
}
