package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.po.DataCheckRule;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.Map;

/**
 * @author tian_y
 */
@Repository
public class DataCheckRuleDaoImpl implements DataCheckRuleDao {


    @Override
    public DataCheckRule getObjectById(Object id) {
        return null;
    }

    @Override
    public void saveNewObject(DataCheckRule checkRule) {

    }

    @Override
    public int updateObject(DataCheckRule checkRule) {
        return 0;
    }

    @Override
    public int deleteObjectById(Object ruleId) {
        return 0;
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc) {
        return null;
    }
}
