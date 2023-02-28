package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;

public interface DatabaseRunTime {
    JSONArray query(String databaseId, String sql, Object[] params);
    JSONArray query(String databaseId, String sql);
    int execute(String databaseId, String sql, Object[] params);
    int execute(String databaseId, String sql);
}
