package com.centit.product.metadata.service;


import com.centit.product.adapter.po.MetaTable;

public interface MetaDataCache {
    MetaTable getTableInfo(String tableId);
    MetaTable getTableInfoWithRelations(String tableId);
    MetaTable getTableInfoWithParents(String tableId);
    MetaTable getTableInfoAll(String tableId);
}
