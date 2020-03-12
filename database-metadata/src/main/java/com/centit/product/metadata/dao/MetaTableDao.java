package com.centit.product.metadata.dao;

import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.algorithm.CollectionsOpt;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
public class MetaTableDao extends BaseDaoImpl<MetaTable, String> {
    @Override
    public Map<String, String> getFilterField() {
        if( filterField == null) {
            filterField = new HashMap<>();
            filterField.put("accessType", CodeBook.IN_HQL_ID);
            filterField.put("tableType", CodeBook.IN_HQL_ID);
            filterField.put("databaseCode", CodeBook.EQUAL_HQL_ID);
            filterField.put("tableId", CodeBook.EQUAL_HQL_ID);
            filterField.put("tableName", CodeBook.LIKE_HQL_ID);
            filterField.put("tableLabelName", CodeBook.LIKE_HQL_ID);
        }
        return  filterField;
    }

    public MetaTable getMetaTable(String databaseCode, String tableName) {
        return super.getObjectByProperties(
            CollectionsOpt.createHashMap("databaseCode",databaseCode,"tableName",tableName));
    }
}
