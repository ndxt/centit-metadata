package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.po.DataCheckRule;
import com.centit.product.metadata.service.DataCheckRuleService;
import com.centit.support.database.utils.PageDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author tian_y
 */
@Service
public class DataCheckRuleServiceImpl implements DataCheckRuleService {

    @Autowired
    private DataCheckRuleDao dataCheckRuleDao;

    @Override
    public void saveNewObject(DataCheckRule checkRule) {
        dataCheckRuleDao.saveNewObject(checkRule);
    }

    @Override
    public DataCheckRule getObjectById(String ruleId) {
        return dataCheckRuleDao.getObjectById(ruleId);
    }

    @Override
    public int updateObject(DataCheckRule checkRule) {
        return dataCheckRuleDao.updateObject(checkRule);
    }

    @Override
    public int deleteObjectById(String ruleId) {
        return dataCheckRuleDao.deleteObjectById(ruleId);
    }

    @Override
    public List<DataCheckRule> listObjectsByProperties(Map<String, Object> properties){
        return dataCheckRuleDao.listObjectsByProperties(properties);
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc) {
        return dataCheckRuleDao.listObjectsByPropertiesAsJson(properties, pageDesc);
    }
}
