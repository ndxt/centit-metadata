package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;

/**
 * 数据库运行时，用于在js组件中暴露数据库操作接口
 * 现在这个已经废弃使用了
 * @author codefan
 */
public interface DatabaseRunTime {
    /**
     * 带参数的sql查询接口
     * @param databaseId 数据库id
     * @param sql sql语句
     * @param params 参数
     * @return 查询结果
     */
    JSONArray query(String databaseId, String sql, Object[] params);

    /**
     * sql查询接口
     * @param databaseId 数据库id
     * @param sql sql语句
     * @return 查询结果
     */
    JSONArray query(String databaseId, String sql);
    /**
     * 带参数的sql执行接口
     * @param databaseId 数据库id
     * @param sql sql语句
     * @param params 参数
     * @return 执行结果
     */
    int execute(String databaseId, String sql, Object[] params);

    /**
     * sql执行接口
     * @param databaseId 数据库id
     * @param sql sql语句
     * @return 执行结果
     */
    int execute(String databaseId, String sql);
}
