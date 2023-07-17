package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.DataCheckRule;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

/**
 * @author tian_y
 */
public interface DataCheckRuleService {

    void saveNewObject(DataCheckRule checkRule);

    DataCheckRule getObjectById(String ruleId);

    int updateObject(DataCheckRule checkRule);

    int deleteObjectById(String ruleId);

    List<DataCheckRule> listObjectsByProperties(Map<String, Object> properties);

    JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc);
}
