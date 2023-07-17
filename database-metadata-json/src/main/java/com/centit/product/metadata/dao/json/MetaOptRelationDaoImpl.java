package com.centit.product.metadata.dao.json;

import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MetaOptRelationDaoImpl implements MetaOptRelationDao {
    @Override
    public void saveNewObject(MetaOptRelation relation) {

    }

    @Override
    public MetaOptRelation getObjectById(Object id) {
        return null;
    }

    @Override
    public MetaOptRelation getObjectByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public int updateObject(MetaOptRelation relation) {
        return 0;
    }

    @Override
    public int deleteObjectById(Object id) {
        return 0;
    }

    @Override
    public List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        return null;
    }
}
