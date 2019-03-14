package com.centit.support.metadata.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.metadata.po.MetaTable;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class MetaTableDao extends BaseDaoImpl<MetaTable, String> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

    public MetaTable getMetaTable(String databaseCode, String tableName) {
        return super.getObjectByProperties(
            CollectionsOpt.createHashMap("databaseCode",databaseCode,"tableName",tableName));
    }
}
