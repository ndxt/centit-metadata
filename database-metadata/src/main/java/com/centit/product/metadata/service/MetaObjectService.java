package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.database.utils.PageDesc;

import java.util.Collection;
import java.util.Map;

/**
 * 根据元数据操作数据库
 */
public interface MetaObjectService {

    MetaTable fetchTableInfo(String tableId);
    /**
     * 根据主键获取数据
     * @param tableId 表ID
     * @param pk 主键
     * @return 返回表数据
     */
    Map<String, Object> getObjectById(String tableId, Map<String, Object> pk);

    /**
     * 获取数据，包括子表信息
     * @param tableId 表ID
     * @param pk 主键
     * @param withChildrenDeep 子表层次
     * @return 返回表数据
     */
    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    /**
     * 获取数据，包括子表信息
     * @param tableId 表ID
     * @param pk 主键
     * @param fields 指定字段
     * @param parents 父表信息
     * @param children 子表信息
     * @return 返回表数据
     */
    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, String [] fields,
                                              String [] parents, String [] children);
    /**
     * 获取表的父表和子表信息
     * @param tableInfo 表ID
     * @param mainObj 表数据
     * @param parents 父表信息
     * @param children 子表信息
     * @return 返回表数据
     */
    Map<String, Object> fetchObjectParentAndChildren(MetaTable tableInfo, Map<String, Object> mainObj,
                                                     String [] parents, String [] children);

    /**
     * 新建一个空对象，没有保存数据库，只是生成所有的自动生成的字段信息
     * @param tableId 表ID
     * @param extParams 生成数据的扩展信息
     * @return 数据库对象
     */
    Map<String, Object> makeNewObject(String tableId, Map<String, Object> extParams);

    /**
     * 新建一个空对象，没有保存数据库，只是生成所有的自动生成的字段信息
     * @param tableId 表ID
     * @return 数据库对象
     */
    Map<String, Object> makeNewObject(String tableId);

    /**
     * 保存数据
     * @param tableId 表ID
     * @param object 数据对象
     * @return 大于0成功
     */
    int saveObject(String tableId, Map<String, Object> object);

    /**
     * 保存数据
     * @param tableId 表ID
     * @param object 数据对象
     * @param extParams 生成数据的扩展信息
     * @return 大于0成功
     */
    int saveObject(String tableId, Map<String, Object> object, Map<String, Object> extParams);

    /**
     * 更新数据
     * @param tableId 表ID
     * @param object 数据对象
     * @return 大于0成功
     */
    int updateObject(String tableId, Map<String, Object> object);

    /**
     * 根据表的指定字段
     * @param tableId 表ID
     * @param fields 指定字段
     * @param object 数据对象
     * @return 大于0成功
     */
    int updateObjectFields(String tableId, final Collection<String> fields,final Map<String, Object> object);

    /**
     * 根据符合条件的表的指定字段
     * @param tableId  表ID
     * @param fields 指定字段
     * @param fieldValues 字段数据
     * @param filterProperties 过滤条件
     * @return 更新记录数量
     */
    int updateObjectsByProperties(String tableId,
                                  final Collection<String> fields,
                                  final Map<String, Object> fieldValues,
                                  final Map<String, Object> filterProperties);

    /**
     * 根据符合条件的表的指定字段
     * @param tableId 表ID
     * @param fieldValues 字段数据
     * @param filterProperties 过滤条件
     * @return 更新记录数量
     */
    int updateObjectsByProperties(String tableId,
                                  final Map<String, Object> fieldValues,
                                  final Map<String, Object> filterProperties);

    /**
     * 删除符合条件的记录
     * @param tableId 表ID
     * @param filterProperties 过滤条件
     * @return 删除的记录数量
     */
    int deleteObjectsByProperties(String tableId,
                                  final Map<String, Object> filterProperties);

    /**
     * 删除数据
     * @param tableId 表ID
     * @param pk 主键数据
     */
    void deleteObject(String tableId, Map<String, Object> pk);

    /**
     * 保存数据，包括数据的子表信息
     * @param tableId 表ID
     * @param object 数据记录
     * @param withChildrenDeep 子表层次
     * @return 大于0成功
     */
    int saveObjectWithChildren(String tableId, Map<String, Object> object,int withChildrenDeep);

    /**
     保存数据，包括数据的子表信息
     * @param tableId 表ID
     * @param object 数据记录
     * @param extParams 扩展数据，用于生成字段默认值
     * @param withChildrenDeep 子表层次
     * @return 大于0成功
     */
    int saveObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep);

    /**
     * 更新数据，包括数据的子表信息
     * @param tableId 表ID
     * @param object 数据记录
     * @param extParams 扩展数据，用于生成字段默认值
     * @param withChildrenDeep 子表层次
     * @return 大于0成功
     */
    int updateObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep);

    /**
     * 更新数据，包括数据的子表信息，更新前检查版本信息是否一致，不一致抛出异常
     * @param tableId 表ID
     * @param object 数据记录
     * @param extParams 扩展数据，用于生成字段默认值
     * @param withChildrenDeep 子表层次
     * @return 大于0成功
     */
    int updateObjectWithChildrenCheckVersion(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep);

    /**
     * 删除数据，包括数据的子表信息
     * @param tableId 表ID
     * @param pk 主键数据
     * @param withChildrenDeep 子表层次
     */
    void deleteObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    /**
     * 逻辑删除数据，包括数据的子表信息， 前提是元数据中保存了逻辑删除信息，如果没有将会抛出异常
     * @param tableId 表ID
     * @param pk 主键数据
     * @param withChildrenDeep 子表层次
     */
    void softDeleteObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    /**
     * 合并数据，包括数据的子表信息
     * @param tableId 表ID
     * @param object 数据记录
     * @param extParams 扩展数据，用于生成字段默认值
     * @param withChildrenDeep 子表层次
     * @return 大于0成功
     */
    int mergeObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep);

    /**
     * 根据属性查询数据
     * @param tableId 表ID
     * @param filter 过滤条件
     * @return 查询结果
     */
    JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter);

    /**
     * 根据属性查询数据 ，分页返回
     * @param tableId 表ID
     * @param params 过滤条件
     * @param pageDesc 分页信息
     * @return 查询结果
     */
    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc);

    /**
     * 根据属性查询数据 ，分页返回
     * @param tableId 表ID
     * @param extFilter 外表条件，一般是权限引擎返回的过滤条件
     * @param params 过滤条件
     * @param fields 指定返回字段
     * @param pageDesc 分页信息
     * @return 查询结果
     */
    JSONArray pageQueryObjects(String tableId, String extFilter, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    /**
     * 根据属性查询数据 ，分页返回
     * @param tableId 表ID
     * @param params 过滤条件
     * @param fields 指定返回字段
     * @param pageDesc 分页信息
     * @return 查询结果
     */
    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    /**
     * 根据sql查询条件语句返回查询结果
     * @param tableId 表ID
     * @param namedSql 条件语句，仅仅是条件部分
     * @param params 过滤条件
     * @param pageDesc 分页信息
     * @return 查询结果
     */
    JSONArray pageQueryObjects(String tableId, String namedSql, Map<String, Object> params, PageDesc pageDesc);

    /**
     * 根据sql查询条件语句返回查询结果， 和上面对比多调用了一次sql语句转换
     * @param tableId 表ID
     * @param paramDriverSql 条件语句，参数驱动sql，仅仅是条件部分
     * @param params 过滤条件
     * @param pageDesc 分页信息
     * @return 查询结果
     */
    JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc);

    /**
     * 获取数据字段的引用数据
     * @param tableId 表的id
     * @param columnCode 对应的字段
     * @param topUnit 租户id
     * @param lang 当前语言
     * @return 数据字典map
     */
    Map<String, String> fetchColumnRefData(String tableId, String columnCode, String topUnit, String lang);
}
