package com.centit.product.metadata.service;

import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface MetaOptRelationService {
    void createMetaOptRelation(MetaOptRelation relation);

    void updateMetaOptRelation(MetaOptRelation relation);

    void deleteMetaOptRelation(String id);

    List<MetaOptRelation> listMetaOptRelation(Map<String, Object> params, PageDesc pageDesc);

    MetaOptRelation getMetaOptRelation(String tableId);

    void batchAddOptRelation(List<MetaOptRelation> relations);
}
