package com.centit.product.dataopt.service.impl;

import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.dataopt.service.MetaObjectService;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.support.dataopt.core.BizModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
@Transactional
public class MetaObjectServiceImpl implements MetaObjectService {
    private Logger logger = LoggerFactory.getLogger(MetaObjectServiceImpl.class);

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    @Autowired
    private MetaTableDao metaTableDao;

    @Autowired
    private MetaColumnDao metaColumnDao;

    @Autowired
    private MetaRelationDao metaRelationDao;


    @Override
    public Map<String, Object> getObjectById(String tableId, Map<String, Object> pk) {
        return null;
    }

    @Override
    public BizModel getObjectById(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        return null;
    }

    @Override
    public int saveObject(String tableId, Map<String, Object> object) {
        return 0;
    }

    @Override
    public int saveObject(String tableId, BizModel object) {
        return 0;
    }
}
