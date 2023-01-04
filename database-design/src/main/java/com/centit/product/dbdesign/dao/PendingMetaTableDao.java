package com.centit.product.dbdesign.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.product.adapter.po.PendingMetaTable;
import com.centit.support.algorithm.NumberBaseOpt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;


/**
 * PendingMdTableDao  Repository.
 * create by scaffold 2016-06-01
 * <p>
 * 未落实表元数据表null
 */

@Repository
public class PendingMetaTableDao extends BaseDaoImpl<PendingMetaTable, String> {

    public static final Log log = LogFactory.getLog(PendingMetaTableDao.class);

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<String, String>();
        filterField.put("tableId", CodeBook.EQUAL_HQL_ID);
        filterField.put("databaseCode", CodeBook.EQUAL_HQL_ID);
        filterField.put("tableName", CodeBook.LIKE_HQL_ID);
        filterField.put("tableLabelName", CodeBook.LIKE_HQL_ID);
        filterField.put("tableType", CodeBook.EQUAL_HQL_ID);
        filterField.put("tableState", CodeBook.EQUAL_HQL_ID);
        filterField.put("tableComment", CodeBook.EQUAL_HQL_ID);
        filterField.put("isInWorkflow", CodeBook.EQUAL_HQL_ID);
        filterField.put("lastModifyDate", CodeBook.EQUAL_HQL_ID);
        filterField.put("recorder", CodeBook.EQUAL_HQL_ID);
        filterField.put("(splitforin)tableNames","upper(table_name) in (:tableNames)");
        filterField.put("(splitforin)databaseCode_in","database_code in (:databaseCode_in)");
        filterField.put("(like)likeTableNameOrLabel", " ( TABLE_NAME LIKE :likeTableNameOrLabel OR TABLE_LABEL_NAME LIKE :likeTableNameOrLabel )");
        return filterField;
    }

    public Long getNextKey() {
        return DatabaseOptUtils.getSequenceNextValue(this, "seq_pendingtableid");
    }

    /**
     * 根据osId过滤MPendingMetaTable数据
     *
     * @param parameters parameters
     * @return JSONArray
     */
    public JSONArray getPendingMetaTableList(Map<String, Object> parameters) {
        String sql = " SELECT  A.DATABASE_NAME, A.SOURCE_TYPE, B.TABLE_ID, B.DATABASE_CODE, B.TABLE_NAME, B.TABLE_LABEL_NAME, B.TABLE_COMMENT, B.TABLE_STATE, B.WORKFLOW_OPT_TYPE AS WORK_FLOW_OPT_TYPE, B.UPDATE_CHECK_TIMESTAMP, B.LAST_MODIFY_DATE,\n" +
            " B.RECORDER, B.PRIMARY_KEY, B.TABLE_TYPE  FROM F_DATABASE_INFO A JOIN F_PENDING_META_TABLE B ON A.DATABASE_CODE = B.DATABASE_CODE " +
            "  WHERE  1 = 1 [ :topUnit | AND A.TOP_UNIT = :topUnit ]  [ :databaseCode | AND A.DATABASE_CODE = :databaseCode ] [ :databaseCode_in | AND A.DATABASE_CODE in (:databaseCode_in) ]" +
            "  [ :(like)tableName | AND B.TABLE_NAME LIKE :tableName ]  [ :(like)tableLabelName | AND B.TABLE_LABEL_NAME LIKE :tableLabelName ] [ :sourceType | AND A.SOURCE_TYPE = :sourceType ] " +
            " [ :(like)likeTableNameOrLabel | AND ( B.TABLE_NAME LIKE :likeTableNameOrLabel OR B.TABLE_LABEL_NAME LIKE :likeTableNameOrLabel  ) ]  ";
        return DatabaseOptUtils.listObjectsByParamsDriverSqlAsJson(this,sql,parameters);
    }

    /**
     * 根据osId过滤MPendingMetaTable数据
     *
     * @param parameters parameters
     * @return JSONArray
     */
    public JSONArray getPendingMetaTableListWithTableOptRelation(Map<String, Object> parameters) {
        String sql = " SELECT A.ID, B.TABLE_ID, B.DATABASE_CODE, B.TABLE_NAME, B.TABLE_LABEL_NAME, B.TABLE_COMMENT, B.TABLE_STATE, B.WORKFLOW_OPT_TYPE AS WORK_FLOW_OPT_TYPE, B.UPDATE_CHECK_TIMESTAMP, B.LAST_MODIFY_DATE,\n" +
            " B.RECORDER, B.PRIMARY_KEY, B.TABLE_TYPE ,C.DATABASE_NAME, C.SOURCE_TYPE FROM  F_TABLE_OPT_RELATION A JOIN F_PENDING_META_TABLE B ON  A.TABLE_ID = B.TABLE_ID " +
            "  JOIN  F_DATABASE_INFO C ON B.DATABASE_CODE =C.DATABASE_CODE  WHERE 1 = 1  [ :optId | AND A.OPT_ID = :optId  ] [ :osId | AND A.OS_ID = :osId  ][ :tableId | AND a.table_ID = :tableId  ] [ :sourceType | AND C.SOURCE_TYPE = :sourceType ] " +
            "  [ :(like)tableName | AND B.TABLE_NAME LIKE :tableName ]  [ :(like)tableLabelName | AND B.TABLE_NAME LIKE :tableLabelName ] [ :(like)likeTableNameOrLabel | AND ( B.TABLE_NAME LIKE :likeTableNameOrLabel OR B.TABLE_LABEL_NAME LIKE :likeTableNameOrLabel  ) ]  ";
        return DatabaseOptUtils.listObjectsByParamsDriverSqlAsJson(this,sql,parameters);
    }


    /**
     *表是否存在
     * @param tableName 表名
     * @param dataBaseCode 数据库code
     * @return boolean
     */
    public boolean isTableExist(String tableName, String dataBaseCode) {
        String sql = " SELECT COUNT(1) FROM F_PENDING_META_TABLE WHERE TABLE_NAME = ? AND DATABASE_CODE = ?  ";
        return NumberBaseOpt.castObjectToInteger(DatabaseOptUtils.getScalarObjectQuery(this, sql, new Object[]{tableName, dataBaseCode})) >0;
    }
}
