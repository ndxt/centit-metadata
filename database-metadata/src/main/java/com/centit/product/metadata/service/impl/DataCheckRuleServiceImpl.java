package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.adapter.po.DataCheckRule;
import com.centit.product.metadata.dao.DataCheckRuleDao;
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
    public List<DataCheckRule> listObjects() {
        return dataCheckRuleDao.listObjects();
    }

    @Override
    public List<DataCheckRule> listObjects(Map<String, Object> map) {
        return dataCheckRuleDao.listObjects(map);
    }

    @Override
    public List<DataCheckRule> listObjects(Map<String, Object> map, PageDesc pageDesc) {
        return dataCheckRuleDao.listObjects(map, pageDesc);

    }

    @Override
    public List<DataCheckRule> listObjectsByProperty(String s, Object o) {
        return dataCheckRuleDao.listObjectsByProperty(s, o);
    }

    @Override
    public List<DataCheckRule> listObjectsByProperties(Map<String, Object> map) {
        return dataCheckRuleDao.listObjectsByProperties(map);
    }

    @Override
    public DataCheckRule getObjectById(String s) {
        return dataCheckRuleDao.getObjectById(s);
    }

    @Override
    public void saveNewObject(DataCheckRule dataCheckRule) {
        dataCheckRuleDao.saveNewObject(dataCheckRule);
    }

    @Override
    public void updateObject(DataCheckRule dataCheckRule) {
        dataCheckRuleDao.updateObject(dataCheckRule);
    }

    @Override
    public void mergeObject(DataCheckRule dataCheckRule) {
        dataCheckRuleDao.mergeObject(dataCheckRule);
    }

    @Override
    public void deleteObject(DataCheckRule dataCheckRule) {
        dataCheckRuleDao.deleteObject(dataCheckRule);
    }

    @Override
    public void deleteObjectById(String s) {
        dataCheckRuleDao.deleteObjectById(s);
    }

    @Override
    public DataCheckRule getObjectByProperty(String s, Object o) {
        return dataCheckRuleDao.getObjectByProperty(s, o);
    }

    @Override
    public DataCheckRule getObjectByProperties(Map<String, Object> map) {
        return dataCheckRuleDao.getObjectByProperties(map);
    }

    @Override
    public JSONArray listObjectsAsJson(Map<String, Object> map, PageDesc pageDesc) {
        return dataCheckRuleDao.listObjectsAsJson(map, pageDesc);
    }

    @Override
    public JSONArray listObjectsBySqlAsJson(String s, Map<String, Object> map, PageDesc pageDesc) {
        return null;
    }
}
