package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.DataCheckRule;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

/**
 * 数据校验规则服务
 * @author tian_y
 */
public interface DataCheckRuleService {
    /**
     * 保存数据校验规则
     * @param checkRule 校验规则
     */
    void saveNewObject(DataCheckRule checkRule);

    /**
     * 获取校验规则
     * @param ruleId 规则id
     * @return 校验规则
     */
    DataCheckRule getObjectById(String ruleId);

    /**
     * 更新数据校验规则
     * @param checkRule 校验规则
     * @return 更新结果
     */
    int updateObject(DataCheckRule checkRule);

    /**
     * 删除校验规则
     * @param ruleId 规则id
     * @return 删除结果
     */
    int deleteObjectById(String ruleId);

    /**
     * 查询校验规则
     * @param properties 查询条件
     * @return 校验规则列表
     */
    List<DataCheckRule> listObjectsByProperties(Map<String, Object> properties);

    /**
     * 分页查询校验规则
     * @param properties 查询条件
     * @param pageDesc 分页信息
     * @return 校验规则列表
     */
    JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc);
}
