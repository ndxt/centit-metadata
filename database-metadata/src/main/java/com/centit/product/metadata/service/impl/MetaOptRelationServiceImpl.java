package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.product.metadata.service.MetaOptRelationService;
import com.centit.support.database.utils.PageDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
@Transactional(rollbackFor = Exception.class)
public class MetaOptRelationServiceImpl implements MetaOptRelationService {

    @Autowired
    MetaOptRelationDao relationDao;

    @Override
    public void createMetaOptRelation(MetaOptRelation relation) {
        relationDao.saveNewObject(relation);
    }

    @Override
    public void updateMetaOptRelation(MetaOptRelation relation) {
        relationDao.updateObject(relation);
    }

    @Override
    public void deleteMetaOptRelation(String tableId) {
        relationDao.deleteObjectById(tableId);
    }

    @Override
    public List<MetaOptRelation> listMetaOptRelation(Map<String, Object> params, PageDesc pageDesc) {
        return relationDao.listObjects(params,pageDesc);
    }

    @Override
    public MetaOptRelation getMetaOptRelation(String tableId) {
        return relationDao.getObjectById(tableId);
    }
}
