package com.centit.product.metadata.dao.json;

import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MetaColumnDaoImpl implements MetaColumnDao {

    @Override
    public MetaColumn getObjectById(MetaColumn id) {
        return null;
    }

    @Override
    public void saveNewObject(MetaColumn columnInfo) {

    }

    @Override
    public int mergeObject(MetaColumn columnInfo) {
        return 0;
    }

    @Override
    public int deleteObject(MetaColumn object) {
        return 0;
    }

    @Override
    public int updateObject(MetaColumn object) {
        return 0;
    }

    @Override
    public List<MetaColumn> listObjectsByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public List<MetaColumn> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        return null;
    }

    @Override
    public List<MetaColumn> listObjectsBySql(String querySql, Object[] params) {
        return null;
    }
}
