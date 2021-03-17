package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Author: Felix
 * Date: 2020/11/3
 * Desc:
 */
@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @RequestMapping("/trademark-sum")
    public Object trademarkSum(String startTime,String endTime,int topN){
        List<Map> rsMap = mySQLService.getTradeAmount(startTime, endTime, topN);
        return rsMap;
    }
}
