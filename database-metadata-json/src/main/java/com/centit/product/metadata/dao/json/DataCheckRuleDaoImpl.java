package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.components.CodeRepositoryCache;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.po.DataCheckRule;
import com.centit.support.common.CachedObject;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author tian_y
 */
@Repository("dataCheckRuleDao")
public class DataCheckRuleDaoImpl implements DataCheckRuleDao {

    @Value("${app.home:./}")
    private String appHome;

    public CachedObject<List<DataCheckRule>> dataCheckRuleRepo  =
        new CachedObject<>(this::loadAllDataCheckRule,
            CodeRepositoryCache.CACHE_EXPIRE_EVERY_DAY );

    private List<DataCheckRule> loadAllDataCheckRule(){
        String ruleFile = appHome + File.separator + "config" + File.separator +  "checkRules.json";
        try {
            JSONArray array = JSON.parseArray(new FileInputStream(ruleFile));
            return array.toJavaList(DataCheckRule.class);
        } catch (IOException e){
            return null;
        }
    }

    @Override
    public DataCheckRule getObjectById(Object id) {
        List<DataCheckRule> allRules = dataCheckRuleRepo.getCachedTarget();
        if(allRules!=null){
            for(DataCheckRule rule : allRules){
                if(StringUtils.equals(rule.getRuleId(), (String) id)){
                    return rule;
                }
            }
        }
        return null;
    }

    @Override
    public void saveNewObject(DataCheckRule checkRule) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int updateObject(DataCheckRule checkRule) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int deleteObjectById(Object ruleId) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<DataCheckRule> listObjectsByProperties(Map<String, Object> properties) {
        return dataCheckRuleRepo.getCachedTarget();
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc) {
        return JSONArray.from(dataCheckRuleRepo.getCachedTarget());
    }
}
