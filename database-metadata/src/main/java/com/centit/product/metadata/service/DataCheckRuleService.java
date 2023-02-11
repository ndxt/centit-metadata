package com.centit.product.metadata.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.jdbc.service.BaseEntityManager;
import com.centit.product.adapter.po.DataCheckRule;
import com.centit.support.database.utils.PageDesc;

import java.util.Map;

/**
 * @author tian_y
 */
public interface DataCheckRuleService extends BaseEntityManager<DataCheckRule, String> {
    JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc);
}
