package com.centit.product.metadata.dao.json;

import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository("metaColumnDao")
public class MetaColumnDaoImpl implements MetaColumnDao {

    @Override
    public MetaColumn getObjectById(MetaColumn id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public void saveNewObject(MetaColumn columnInfo) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int mergeObject(MetaColumn columnInfo) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int deleteObject(MetaColumn object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public int updateObject(MetaColumn object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaColumn> listObjectsByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaColumn> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }

    @Override
    public List<MetaColumn> listObjectsBySql(String querySql, Object[] params) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "Runtime 运行时环境，不支持元数据的修改!");
    }
}
