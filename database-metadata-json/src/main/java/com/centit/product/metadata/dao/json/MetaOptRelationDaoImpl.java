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
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public MetaOptRelation getObjectById(Object id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public MetaOptRelation getObjectByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int updateObject(MetaOptRelation relation) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int deleteObjectById(Object id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }
}
