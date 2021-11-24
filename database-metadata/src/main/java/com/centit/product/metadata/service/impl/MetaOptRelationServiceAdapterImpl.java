package com.centit.product.metadata.service.impl;

import com.centit.product.dubbo.adapter.MetaOptRelationService;
import com.centit.product.metadata.dao.MetaOptRelationDao;
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
import java.util.stream.Collectors;
@Service
public class MetaOptRelationServiceAdapterImpl implements MetaOptRelationService {

    protected Logger logger = LoggerFactory.getLogger(MetaOptRelationService.class);

    @Autowired
    MetaOptRelationDao relationDao;

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
