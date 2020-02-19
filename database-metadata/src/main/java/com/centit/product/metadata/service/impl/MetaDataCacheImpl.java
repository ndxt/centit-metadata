package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataCache;
import com.centit.support.common.CachedMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetaDataCacheImpl implements MetaDataCache {

    /**
     * Integer 11 parent,relation
     */
    private CachedMap<String, MutablePair<Integer, MetaTable>> metaTableCache =
        new CachedMap<>(
            ( tableId )-> new MutablePair<>(0, this.getTableInfoWithColumns(tableId)),
            10);

    @Autowired
    private MetaTableDao metaTableDao;

    @Autowired
    private MetaRelationDao metaRelationDao;

    private MetaTable getTableInfoWithColumns(String tableId){
        MetaTable metaTable = this.metaTableDao.getObjectById(tableId);
        metaTableDao.fetchObjectReference(metaTable, "mdColumns");//mdRelations
        return metaTable;
    }

    public MetaTable getTableInfo(String tableId){
        //metaTableCache.setFreshData();
        if (metaTableCache.getCachedValue(tableId)!=null) {
            return metaTableCache.getCachedValue(tableId).getRight();
        } else{
            return null;
        }
    }

    private MetaTable fetchTableRelations(MetaTable metaTable){
        metaTableDao.fetchObjectReference(metaTable, "mdRelations");
        if (metaTable.getMdRelations().size() > 0) {
            for (MetaRelation mr : metaTable.getMdRelations()) {
                metaRelationDao.fetchObjectReference(mr, "relationDetails");
            }
        }
        return metaTable;
    }

    private MetaTable fetchTableParents(MetaTable metaTable){
        metaTableDao.fetchObjectReference(metaTable, "parents");
        if( metaTable.getParents().size()>0) {
            for (MetaRelation parent : metaTable.getParents()){
                metaRelationDao.fetchObjectReference(parent, "relationDetails");
            }
        }
        return metaTable;
    }

    public MetaTable getTableInfoWithRelations(String tableId){
        MutablePair<Integer, MetaTable> tablePair = metaTableCache.getCachedValue(tableId);
        MetaTable metaTable = tablePair.getRight();
        if(tablePair.getLeft() % 10 == 0){
            fetchTableRelations(metaTable);
            tablePair.setLeft(tablePair.getLeft() + 1);
        }
        return metaTable;
    }

    public MetaTable getTableInfoWithParents(String tableId){
        MutablePair<Integer, MetaTable> tablePair = metaTableCache.getCachedValue(tableId);
        MetaTable metaTable = tablePair.getRight();
        if(tablePair.getLeft() / 10 == 0){
            fetchTableParents(metaTable);
            tablePair.setLeft(tablePair.getLeft() +10);
        }
        return metaTable;
    }

    public MetaTable getTableInfoAll(String tableId){
        MutablePair<Integer, MetaTable> tablePair = metaTableCache.getCachedValue(tableId);
        MetaTable metaTable = tablePair.getRight();

        if(tablePair.getLeft() % 10 == 0){
            fetchTableRelations(metaTable);
        }
        if(tablePair.getLeft() / 10 == 0){
            fetchTableParents(metaTable);
        }
        tablePair.setLeft(11);
        return metaTable;
    }
}
