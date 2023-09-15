package com.centit.product.metadata.service;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.vo.MetaTableCascade;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.PageDesc;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * 元数据维护信息
 */
public interface MetaDataService {

    /**
     * 获取一个数据库下的所有表格元数据
     * @param databaseCode 数据库id
     * @return 元数据列表
     */
    List<MetaTable> listAllMetaTables(String databaseCode);
    /**
     * 获取一个数据库下的所有表格元数据
     * @param databaseCode 数据库id
     * @return 元数据列表， 包括字段信息
     */
    List<MetaTable> listAllMetaTablesWithDetail(String databaseCode);

    /**
     * 分页查询元数据信息
     * @param filterMap 查询条件
     * @param pageDes 分页信息
     * @return 元数据列表
     */
    JSONArray listMetaTables(Map<String, Object> filterMap, PageDesc pageDes);

    /**
     * 获取一个数据源信息
     * @param databaseCode 数据库id
     * @return 数据源信息
     */
    SourceInfo getDatabaseInfo(String databaseCode);

    /**
     * 列举业务系统关联的数据源信息
     * @param osId 业务系统id
     * @return 数据源信息
     */
    List<SourceInfo> listDatabase(String osId);

    /**
     * 根据传入的过滤条件获取资源库信息
     * @param map 过滤条件
     * @return 数据源列表信息
     */
    List<SourceInfo> listDatabase(Map<String,Object> map);

    /**
     * 获取数据源中的数据库列表，反向工程
     * @param databaseCode 数据源id
     * @return 数据库中的表结构
     */
    List<SimpleTableInfo> listRealTablesWithoutColumn(String databaseCode);

    /**
     * 同步数据库信息，反向工程
     * @param databaseCode 数据源id
     * @param recorder 操作人员
     * @param tableNames 同步的表格
     */
    void syncDb(String databaseCode, String recorder, String[] tableNames);

    /**
     * 同步数据库中单个表信息，反向工程
     * @param databaseCode 数据源id
     * @param recorder 操作人员
     * @param tableName 同步的表格
     * @param tableId 指定ID，这个是可选的
     */
    void syncSingleTable(String databaseCode, String recorder, String tableName, @Nullable String tableId);

    /**
     * 更新元数据信息
     * @param metaTable 元数据信息
     */
    void updateMetaTable(MetaTable metaTable);

    /**
     * 获取元数据信息
     * @param tableId 表的id
     * @return 表的元数据信息
     */
    MetaTable getMetaTable(String tableId);

    /**
     * 获取元数据信息，包括字段字表等信息
     * @param tableId 表的id
     * @return 表的元数据信息
     */
    MetaTable getMetaTableWithRelations(String tableId);

    /**
     * 根据数据库id和表名称获取元数据信息
     * @param databaseCode 数据库ID
     * @param tableName 表名
     * @return 表的元数据信息
     */
    MetaTable getMetaTable(String databaseCode, String tableName);

    /**
     * 根据数据库id和表名称获取元数据信息，包括字段字表等信息
     * @param databaseCode 数据库ID
     * @param tableName 表名
     * @return 表的元数据信息
     */
    MetaTable getMetaTableWithRelations(String databaseCode, String tableName);

    /**
     * 获取表的关联关系信息
     * @param condition 查询条件
     * @param pageDesc 分页信息
     * @return 关联关系列表
     */
    List<MetaRelation> listMetaRelation(Map<String, Object> condition, PageDesc pageDesc);

    /**
     * 获取具体的表的关联关系信息
     * @param tableId 表id
     * @param pageDesc 分页信息
     * @return 关联关系列表
     */
    List<MetaRelation> listMetaRelation(String tableId, PageDesc pageDesc);

    /**
     * 获取表的字段信息
     * @param tableId 表id
     * @param pageDesc 分页返回字段
     * @return 字段列表
     */
    List<MetaColumn> listMetaColumns(String tableId, PageDesc pageDesc);

    /**
     * 获取表的字段信息
     * @param tableId 表id
     * @return 字段列表
     */
    List<MetaColumn> listMetaColumns(String tableId);
    /**
     * 获取关联详细信息
     * @param relationId 关联id
     * @return 关联信息
     */
    MetaRelation getMetaRelationById(String relationId);

    /**
     * 根据名称获取关系信息
     * @param tableId 表的id
     * @param relationName 关联名称
     * @return 关联信息
     */
    MetaRelation getMetaRelationByName(String tableId, String relationName);

    /**
     * 获取关联列表
     * @param tableId 表id
     * @return 表的子表信息
     */
    List<MetaRelation> listMetaRelation(String tableId);

    /**
     * 保存关联信息
     * @param relation 关联信息
     */
    void createRelation(MetaRelation relation);

    /**
     * 保存一个表的所有关联信息
     * @param tableId 表id
     * @param relations 关联列表
     */
    void saveRelations(String tableId, List<MetaRelation> relations);

    /**
     * 获取字段详细信息
     * @param tableId 表Id
     * @param columnName 字段名称
     * @return 字段详细信息
     */
    MetaColumn getMetaColumn(String tableId, String columnName);

    /**
     * 更改字段详细信息
     * @param metaColumn 字段信息
     */
    void updateMetaColumn(MetaColumn metaColumn);

    /**
     * 获取元数据的VTO信息
     * @param databaseCode 数据库id
     * @param tableCode 表代码
     * @return 返回前端要暂时的数据
     */
    MetaTableCascade getMetaTableCascade(String databaseCode, String tableCode);

    /**
     * 从TableStore导入元数据
     * @param databaseCode 数据库id
     * @param jsonObject TableStore中的元数据
     * @param userCode 操作人员
     */
    void importRelationFromTableStore(String databaseCode, JSONObject jsonObject, String userCode);
}
