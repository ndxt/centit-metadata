package com.centit.product.metadata.dao.json;

import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository("metaRelationDao")
public class MetaRelationDaoImpl implements MetaRelationDao {

    @Override
    public MetaRelation getObjectById(Object id) {
        return null;
    }

    @Override
    public void saveNewObject(MetaRelation object) {

    }

    @Override
    public int deleteObject(MetaRelation object) {
        return 0;
    }

    @Override
    public int updateObject(MetaRelation object) {
        return 0;
    }

    @Override
    public int saveObjectReference(MetaRelation object, String columnName) {
        return 0;
    }

    @Override
    public int saveObjectReferences(MetaRelation object) {
        return 0;
    }

    @Override
    public int deleteObjectReference(MetaRelation object, String columnName) {
        return 0;
    }

    @Override
    public MetaRelation getObjectByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public List<MetaRelation> listRelationByTables(String parentTableId, String childTableId) {
        return null;
    }

    @Override
    public List<MetaRelation> listObjectsByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public List<MetaRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        return null;
    }

    @Override
    public MetaRelation fetchObjectReference(MetaRelation object, String columnName) {
        return null;
    }

    @Override
    public MetaRelation fetchObjectReferences(MetaRelation object) {
        return null;
    }
}
