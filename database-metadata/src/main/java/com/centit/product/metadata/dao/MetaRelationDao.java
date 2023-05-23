package com.centit.product.metadata.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.adapter.po.MetaRelation;
import com.centit.support.algorithm.CollectionsOpt;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MetaRelationDao extends BaseDaoImpl<MetaRelation, Long> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

    public List<MetaRelation> listRelationByTables(String parentTableId, String childTableId){
        return this.listObjectsByProperties(
            CollectionsOpt.createHashMap("parentTableId", parentTableId, "childTableId", childTableId)
        );
    }
}
