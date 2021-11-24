package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.product.metadata.service.MetaOptRelationService;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.database.utils.PageDesc;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Transactional(rollbackFor = Exception.class)
public class MetaOptRelationServiceImpl implements MetaOptRelationService {

    protected Logger logger = LoggerFactory.getLogger(MetaOptRelationService.class);

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
    public void deleteMetaOptRelation(String id) {
        relationDao.deleteObjectById(id);
    }

    @Override
    public List<MetaOptRelation> listMetaOptRelation(Map<String, Object> params, PageDesc pageDesc) {
        return relationDao.listObjects(params,pageDesc);
    }

    @Override
    public MetaOptRelation getMetaOptRelation(String tableId) {
        return relationDao.getObjectById(tableId);
    }

    @Override
    @Transactional
    public void batchAddOptRelation(List<MetaOptRelation> relations) {
        for (MetaOptRelation relation : relations) {
            MetaOptRelation metaOptRelation = relationDao.getObjectByProperties(CollectionsOpt.createHashMap("optId", relation.getOptId(),
                "tableId", relation.getTableId()));
            if (null != metaOptRelation){
                relation.setId(metaOptRelation.getId());
                relationDao.updateObject(metaOptRelation);
            }else {
                relationDao.saveNewObject(relation);
            }
        }
    }

    @Override
    @Transactional
    public int deleteMetaOptRelationByOptIds(String optIds) {
        if (StringUtils.isBlank(optIds)) {
            logger.info("根据optIds删除关联关系接口参数optIds为空。");
            return 0;
        }
        List<MetaOptRelation> metaOptRelations = relationDao.listObjects(CollectionsOpt.createHashMap("optIds", optIds));
        if (CollectionUtils.isEmpty(metaOptRelations)) {
            logger.info("根据optIds未查询到关联信息。optIds:{}", optIds);
            return 0;
        }
        List<String> ids = metaOptRelations.stream().map(MetaOptRelation::getId).collect(Collectors.toList());
        for (String id : ids) {
            relationDao.deleteObjectById(id);
        }
        return ids.size();
    }
}
