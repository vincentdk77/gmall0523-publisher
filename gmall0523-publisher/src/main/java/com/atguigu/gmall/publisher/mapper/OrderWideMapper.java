package com.atguigu.gmall.publisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Author: Felix
 * Date: 2020/11/2
 * Desc: 对订单宽表进行操作的接口
 */
public interface OrderWideMapper {
    //获取指定日期的交易额
    BigDecimal selectOrderAmountTotal(String date);

    //获取指定日期的分时交易额
    List<Map> selectOrderAmountHour(String date);
}
