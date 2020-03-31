package com.centit.product.datapacket.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.datapacket.po.DataSetDefine;
import com.centit.product.datapacket.vo.ColumnSchema;
import com.centit.support.database.utils.PageDesc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataSetDefineService {

    /**
     * 新增数据库查询
     */
    void createDbQuery(DataSetDefine dataSetDefine);

    void updateDbQuery(DataSetDefine dataSetDefine);

    void deleteDbQuery(String queryId);

    List<DataSetDefine> listDbQuery(Map<String, Object> params, PageDesc pageDesc);

    DataSetDefine getDbQuery(String queryId);

    List<ColumnSchema> generateSqlFields(String databaseCode, String sql, Map<String, Object> params);
    List<ColumnSchema> generateExcelFields(Map<String, Object> params);
    List<ColumnSchema> generateCsvFields(Map<String, Object> params);
    List<ColumnSchema> generateJsonFields(Map<String, Object> params);
    JSONArray queryViewSqlData(String databaseCode, String sql, Map<String, Object> params);

    Set<String> generateSqlParams(String sql);

}
