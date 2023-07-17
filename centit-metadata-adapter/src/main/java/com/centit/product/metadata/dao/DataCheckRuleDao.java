package com.centit.product.metadata.dao;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.DataCheckRule;
import com.centit.support.database.utils.PageDesc;

import java.util.Map;

/**
 * @author tian_y
 */

public interface DataCheckRuleDao {

    DataCheckRule getObjectById(Object id);

    void saveNewObject(DataCheckRule checkRule);

    int updateObject(DataCheckRule checkRule);

    int deleteObjectById(Object ruleId);

    JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc);
}
