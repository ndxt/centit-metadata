package com.centit.product.test;

import com.alibaba.fastjson.JSON;
import com.centit.support.dataopt.core.DataSet;
import com.centit.support.dataopt.core.SimpleDataSet;
import com.centit.support.dataopt.utils.DataSetOptUtil;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class TestDataSetOptUtils {
    public static void main(String[] args) {
        List<Map<String, Object>> list = new ArrayList<>();

        Map<String, Object> map1 = new HashMap<>();
        map1.put("century", "21");
        map1.put("year", "2019");
        map1.put("season", 4);
        map1.put("month", 11);
        map1.put("seal", 36);
        list.add(map1);
        for (int i = 0; i < 3; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("century", "21");
            map.put("year", "2019");
            map.put("season", i + 1);
            map.put("month", (i + 1) * 3);
            map.put("seal", 30 + i);
            list.add(map);
        }
        for (int i = 0; i < 4; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("century", "21");
            map.put("year", "2018");
            map.put("season", i + 1);
            map.put("month", (i + 1) * 3);
            map.put("seal", 10 + i);
            list.add(map);
        }

        List<String> rowHeader = new ArrayList<>();
        List<String> colHeader = new ArrayList<>();
        rowHeader.add("century");
        rowHeader.add("year");
        colHeader.add("season");
        colHeader.add("month");
        SimpleDataSet dataSet = new SimpleDataSet();
        dataSet.setData(list);
        System.out.println("=================cross===================");
        System.out.println(JSON.toJSONString(dataSet.getData()));
        DataSet result = DataSetOptUtil.crossTabulation(dataSet, rowHeader, colHeader);
        System.out.println(JSON.toJSONString(result.getData()));

        List<Map<String, Object>> list1 = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("year", "2018");
            map.put("season", 1);
            map.put("income", 1+i);
            map.put("seal", i);
            list1.add(map);
        }
        for (int i = 0; i < 4; i++) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("year", "2018");
            map.put("season", 2);
            map.put("income", 2+i);
            map.put("seal", 1 + i);
            list1.add(map);
        }
        for (int i = 0; i < 4; i++) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("year", "2018");
            map.put("season", 1);
            map.put("income", 1+i);
            map.put("seal", i);
            list1.add(map);
        }

        List<String> groupByFields = new ArrayList<>();
        groupByFields.add("year");
        groupByFields.add("season");
        SimpleDataSet dataSet1 = new SimpleDataSet();
        dataSet1.setData(list1);
        Map<String, String> statDesc = new HashMap<>();
        System.out.println("===================group by========================");
        System.out.println(JSON.toJSONString(dataSet1.getData()));
        statDesc.put("income","sum");
        DataSet result1 = DataSetOptUtil.statDataset2(dataSet1, groupByFields,statDesc);
        System.out.println(JSON.toJSONString(result1.getData()));
    }
}
