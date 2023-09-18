package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.product.metadata.service.MetaOptRelationService;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.database.utils.PageDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
@Transactional(rollbackFor = Exception.class)
public class MetaOptRelationServiceImpl implements MetaOptRelationService {

    protected Logger logger = LoggerFactory.getLogger(MetaOptRelationService.class);

    @Autowired
    MetaOptRelationDao metaOptRelationDao;

    @Override
    public void createMetaOptRelation(MetaOptRelation relation) {
        metaOptRelationDao.saveNewObject(relation);
    }

    @Override
    public void updateMetaOptRelation(MetaOptRelation relation) {
        metaOptRelationDao.updateObject(relation);
    }

    @Override
    public void deleteMetaOptRelation(String relationId) {
        metaOptRelationDao.deleteObjectById(relationId);
    }

    @Override
    public List<MetaOptRelation> listMetaOptRelation(Map<String, Object> params, PageDesc pageDesc) {
        return metaOptRelationDao.listObjectsByProperties(params,pageDesc);
    }

    @Override
    public MetaOptRelation getMetaOptRelation(String relationId) {
        return metaOptRelationDao.getObjectById(relationId);
    }

    @Override
    @Transactional
    public void batchAddOptRelation(List<MetaOptRelation> relations) {
        for (MetaOptRelation relation : relations) {
            MetaOptRelation metaOptRelation = metaOptRelationDao.getObjectByProperties(CollectionsOpt.createHashMap("optId", relation.getOptId(),
                "tableId", relation.getTableId()));
            if (null != metaOptRelation){
                relation.setId(metaOptRelation.getId());
                metaOptRelationDao.updateObject(metaOptRelation);
            }else {
                metaOptRelationDao.saveNewObject(relation);
            }
        }
    }
}
