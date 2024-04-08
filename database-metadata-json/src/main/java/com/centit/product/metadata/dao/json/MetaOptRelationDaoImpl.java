package com.centit.product.metadata.dao.json;

import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository("metaOptRelationDao")
public class MetaOptRelationDaoImpl implements MetaOptRelationDao {
    @Override
    public void saveNewObject(MetaOptRelation relation) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public MetaOptRelation getObjectById(Object id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public MetaOptRelation getObjectByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int updateObject(MetaOptRelation relation) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int deleteObjectById(Object id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }
}
