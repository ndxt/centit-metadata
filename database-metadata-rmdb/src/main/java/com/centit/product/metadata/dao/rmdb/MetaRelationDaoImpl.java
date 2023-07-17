package com.centit.product.metadata.dao.rmdb;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.support.algorithm.CollectionsOpt;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository("metaRelationDao")
public class MetaRelationDaoImpl extends BaseDaoImpl<MetaRelation, Long> implements MetaRelationDao {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

    @Override
    public List<MetaRelation> listRelationByTables(String parentTableId, String childTableId){
        return this.listObjectsByProperties(
            CollectionsOpt.createHashMap("parentTableId", parentTableId, "childTableId", childTableId)
        );
    }
}
