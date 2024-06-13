package com.centit.product.metadata.dao.json;

import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository("metaRelationDao")
public class MetaRelationDaoImpl implements MetaRelationDao {

    @Override
    public MetaRelation getObjectById(Object id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public void saveNewObject(MetaRelation object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int deleteObject(MetaRelation object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int updateObject(MetaRelation object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int saveObjectReference(MetaRelation object, String columnName) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int saveObjectReferences(MetaRelation object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int deleteObjectReference(MetaRelation object, String columnName) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public MetaRelation getObjectByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaRelation> listRelationByTables(String parentTableId, String childTableId) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaRelation> listObjectsByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public MetaRelation fetchObjectReference(MetaRelation object, String columnName) {
        return object;
    }

    @Override
    public MetaRelation fetchObjectReferences(MetaRelation object) {
        return object;
    }
}
