package com.centit.product.metadata.service;

import com.centit.product.metadata.po.MetaTable;

/**
 * 元数据缓存接口
 * @author codefan
 */
public interface MetaDataCache {
    /**
     * 获取元数据
     * @param tableId 表id
     * @return 表的元数据
     */
    MetaTable getTableInfo(String tableId);

    /**
     * 获取元数据，包括字段和子表信息
     * @param tableId 表id
     * @return 表的元数据
     */
    MetaTable getTableInfoWithRelations(String tableId);

    /**
     * 获取元数据，包括父表信息
     * @param tableId 表id
     * @return 表的元数据
     */
    MetaTable getTableInfoWithParents(String tableId);

    /**
     * 获取元数据，包括字段、子表和父表信息
     * @param tableId 表id
     * @return 表的元数据
     */
    MetaTable getTableInfoAll(String tableId);
}
