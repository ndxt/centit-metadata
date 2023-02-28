package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.jdbc.service.BaseEntityManagerImpl;
import com.centit.product.adapter.po.DataCheckRule;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.service.DataCheckRuleService;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * @author tian_y
 */
@Service
public class DataCheckRuleServiceImpl extends BaseEntityManagerImpl<DataCheckRule,
    String, DataCheckRuleDao>  implements DataCheckRuleService {

    private DataCheckRuleDao dataCheckRuleDao;

    @Resource(name = "dataCheckRuleDao")
    @NotNull
    public void setDataCheckRuleDao(DataCheckRuleDao baseDao)
    {
        this.dataCheckRuleDao = baseDao;
        setBaseDao(this.dataCheckRuleDao);
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc) {
        return dataCheckRuleDao.listObjectsByPropertiesAsJson(properties, pageDesc);
    }
}
