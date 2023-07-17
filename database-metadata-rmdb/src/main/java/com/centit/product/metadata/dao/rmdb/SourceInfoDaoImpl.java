package com.centit.product.metadata.dao.rmdb;

import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.framework.jdbc.dao.JdbcTemplateUtils;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.database.utils.QueryAndParams;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class SourceInfoDaoImpl extends BaseDaoImpl<SourceInfo, String> implements SourceInfoDao{

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("databaseName", CodeBook.LIKE_HQL_ID);
        filterField.put("databaseCode", CodeBook.EQUAL_HQL_ID);
        filterField.put("osId", CodeBook.EQUAL_HQL_ID);
        filterField.put("databaseType", CodeBook.LIKE_HQL_ID);
        filterField.put("hostPort", CodeBook.LIKE_HQL_ID);
        filterField.put("databaseUrl", CodeBook.LIKE_HQL_ID);
        filterField.put("username", CodeBook.LIKE_HQL_ID);
        filterField.put("password", CodeBook.LIKE_HQL_ID);
        filterField.put("dataDesc", CodeBook.LIKE_HQL_ID);
        filterField.put("createTime", CodeBook.LIKE_HQL_ID);
        filterField.put("created", CodeBook.LIKE_HQL_ID);
        filterField.put("sourceType", CodeBook.EQUAL_HQL_ID);
        filterField.put("topUnit",CodeBook.EQUAL_HQL_ID);
        filterField.put("databaseCodes","Database_Code in (:databaseCodes)");
        return filterField;
    }

    @Override
    public List<SourceInfo> listDatabase() {
        return this.listObjects();
    }

    @Override
    public List<SourceInfo> listDatabaseByOsId(String osId) {
        return super.listObjectsByProperties(
            CollectionsOpt.createHashMap("osId",osId));
    }

    @Override
    public SourceInfo getDatabaseInfoById(String databaseCode) {
        return this.getObjectById(databaseCode);
    }

    @Override
    public String getNextKey() {
        return StringBaseOpt.fillZeroForString(
            String.valueOf(
                JdbcTemplateUtils.getSequenceNextValue(
                    this.jdbcTemplate, "S_DATABASECODE")), 10);
    }

    @Override
    public JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc) {
        if (StringUtils.isBlank(databaseName)) {
            return super.listObjectsByPropertiesAsJson(new HashMap<>(1), pageDesc);
        }
        String matchStr = QueryUtils.getMatchString(databaseName);
        return super.listObjectsByFilterAsJson("where DATABASE_NAME like ? or DATABASE_URL like ?", new Object[]{matchStr, matchStr}, pageDesc);
    }

    /**
     * 统计租户下数据个数
     * @param params 过滤参数
     * @return 统计租户下数据个数
     */
    @Override
    public int countDataBase(Map<String,Object> params){
        String sql = "  SELECT COUNT(1) COUNT FROM F_DATABASE_INFO WHERE 1 = 1 [ :topUnit | AND TOP_UNIT = :topUnit ]  [ :sourceType |  AND SOURCE_TYPE  = :sourceType ]  ";
        QueryAndParams queryAndParams = QueryAndParams.createFromQueryAndNamedParams(QueryUtils.translateQuery(sql, params));
        return NumberBaseOpt.castObjectToInteger(DatabaseOptUtils.getScalarObjectQuery(this, queryAndParams.getQuery(),queryAndParams.getParams()));
    }
}
