package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

/**
 * 数据与服务资源维护信息
 */
public interface SourceInfoManager {
    /**
     * 获取起源信息
     * @param databaseCode 资源主键
     * @return 资源代码
     */
    SourceInfo getObjectById(String databaseCode);

    /**
     * 删除数据资源信息
     * @param databaseCode 主键
     * @return 是否删除成功
     */
    int deleteObjectById(String databaseCode);

    /**
     * 返回所有数据源资源
     * @return 所有数据源资源
     */
    List<SourceInfo> listDatabase();

    /**
     * 保存数据与服务资源信息
     * @param sourceInfo 数据源信息
     */
    void saveNewObject(SourceInfo sourceInfo);

    /**
     * 合并保存数据与服务资源信息
     * @param sourceInfo 数据源信息
     */
    void mergeObject(SourceInfo sourceInfo);

    /**
     * 返回数据源资源池
     * @return 数据源资源池
     */
    Map<String, SourceInfo> listDatabaseToDBRepo();

    /**
     * 查询数据源信息
     * @param map 过滤条件
     * @return 数据源列表
     */
    List<SourceInfo> listObjects(Map<String, Object> map);

    /**
     * 查询数据源信息
     * @param filterMap 过滤条件
     * @param pageDesc 分页信息
     * @return 数据源列表
     */
    JSONArray listDatabaseAsJson(Map<String, Object> filterMap, PageDesc pageDesc);

    void appendRelativeOsInfo(JSONArray sourceInfoList);
    /**
     * 根据数据库名称 模糊查找数据源
     * @param databaseName 数据库名或者url中的信息
     * @param pageDesc 分页查询
     * @return 数据源列表
     */
    JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc);

    /**
     * 查看应用关联的资源信息
     * @param osId 应用ID
     * @return 资源信息
     */
    List<SourceInfo> listDatabaseByOsId(String osId);

}

