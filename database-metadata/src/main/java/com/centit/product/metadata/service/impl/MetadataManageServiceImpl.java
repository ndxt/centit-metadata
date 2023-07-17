package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.api.MetadataManageService;
import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.support.algorithm.CollectionsOpt;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MetadataManageServiceImpl implements MetadataManageService {

    @Autowired
    private SourceInfoDao sourceInfoDao;


    protected Logger logger = LoggerFactory.getLogger(MetadataManageService.class);

    @Autowired
    MetaOptRelationDao relationDao;

    @Override
    @Transactional
    public int deleteMetaOptRelationByOptId(String optId) {
        if (StringUtils.isBlank(optId)) {
            logger.info("根据optId删除关联关系接口参数optId为空。");
            return 0;
        }
        List<MetaOptRelation> metaOptRelations =
            relationDao.listObjectsByProperties(CollectionsOpt.createHashMap("optId", optId));
        if (CollectionUtils.isEmpty(metaOptRelations)) {
            logger.info("根据optId未查询到关联信息。optId:{}", optId);
            return 0;
        }
        Set<String> ids = metaOptRelations.stream().map(MetaOptRelation::getId).collect(Collectors.toSet());
        for (String id : ids) {
            relationDao.deleteObjectById(id);
        }
        return ids.size();
    }

    @Override
    public int countDataBase(Map<String,Object> params) {

        return sourceInfoDao.countDataBase(params);
    }
}
