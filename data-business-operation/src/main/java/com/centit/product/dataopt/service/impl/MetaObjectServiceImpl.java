package com.centit.product.dataopt.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.ObjectException;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.dataopt.service.MetaObjectService;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.utils.JdbcConnect;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.utils.FieldType;
import com.centit.support.database.utils.TransactionHandler;
import com.centit.support.dataopt.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Transactional
public class MetaObjectServiceImpl implements MetaObjectService {
    private Logger logger = LoggerFactory.getLogger(MetaObjectServiceImpl.class);

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    @Autowired
    private MetaTableDao metaTableDao;

    @Autowired
    private MetaColumnDao metaColumnDao;

    @Autowired
    private MetaRelationDao metaRelationDao;

    private MetaTable fetchTableInfo(String tableId, boolean fetchRelattions){
        MetaTable metaTable = metaTableDao.getObjectById(tableId);
        metaTableDao.fetchObjectReference(metaTable, "mdColumns");//mdRelations
        if(fetchRelattions){
            metaTableDao.fetchObjectReference(metaTable, "mdRelations");
            if( metaTable.getMdRelations() != null) {
                for (MetaRelation mr : metaTable.getMdRelations()){
                    metaRelationDao.fetchObjectReference(mr, "relationDetails");
                }
            }
        }
        //metaTableDao.fetchObjectReferences(metaTable);
        return metaTable;
    }

    private DatabaseInfo fetchDatabaseInfo(String databaseCode){
        return integrationEnvironment.getDatabaseInfo(databaseCode);
    }

    @Override
    public Map<String, Object> getObjectById(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).getObjectById(pk));
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object>  getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = fetchTableInfo(tableId,true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    Map<String, Object> mainObj =
                        GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).getObjectById(pk);
                    List<MetaRelation> mds = tableInfo.getMdRelations();
                    if(mds!=null) {
                        for (MetaRelation md : mds) {
                            MetaTable relTableInfo = fetchTableInfo(md.getChildTableId(),true);
                            Map<String, Object> ref = new HashMap<>();
                            for(Map.Entry<String, String> rc : md.getReferenceColumns().entrySet()){
                                ref.put(rc.getValue(), mainObj.get(rc.getKey()));
                            }
                            JSONArray ja = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                .listObjectsByProperties(ref);
                            mainObj.put(FieldType.mapPropName(relTableInfo.getTableName()), ja);
                        }
                    }
                    return mainObj;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }
    /**
     * @param tableId 表ID
     * @param pk 组件
     * @param withChildrenDeep 目前可以只考虑 一层 就是 只能为 1
     * @return
     */
    @Override
    public BizModel getObjectAsBizMode(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = fetchTableInfo(tableId,true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    SimpleBizModel bizModel = new SimpleBizModel(tableInfo.getTableName());
                    Map<String, Object> mainObj =
                        GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).getObjectById(pk);
                    SimpleDataSet dataSet = new SingleRowDataSet(mainObj);
                    dataSet.setDataSetName(tableInfo.getTableName());
                    bizModel.setMainDataSet(dataSet);
                    List<MetaRelation> mds = tableInfo.getMdRelations();
                    if(mds!=null) {
                        for (MetaRelation md : mds) {
                            MetaTable relTableInfo = fetchTableInfo(md.getChildTableId(),true);
                            Map<String, Object> ref = new HashMap<>();
                            for(Map.Entry<String, String> rc : md.getReferenceColumns().entrySet()){
                                ref.put(rc.getValue(), mainObj.get(rc.getKey()));
                            }
                            JSONArray ja = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo).listObjectsByProperties(ref);
                            SimpleDataSet subDataSet = new SimpleDataSet((List)ja);
                            subDataSet.setDataSetName(relTableInfo.getTableName());
                            bizModel.addDataSet(subDataSet);
                        }
                    }
                    return bizModel;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int saveObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).saveNewObject(object));
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).updateObject(object));
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int mergeObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).mergeObject(object));
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    public int inndrsSaveObject(String tableId, BizModel bizModel, boolean isMerge) {
        MetaTable tableInfo = fetchTableInfo(tableId, true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction( JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    DataSet ds = bizModel.getMainDataSet();
                    Map<String, Object> mainObj = ds.getFirstRow();
                    if(isMerge) {
                        GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).mergeObject(mainObj);
                    }else {
                        GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).saveNewObject(mainObj);
                    }
                    List<MetaRelation> mds = tableInfo.getMdRelations();
                    if(mds!=null) {
                        for (MetaRelation md : mds) {
                            MetaTable relTableInfo = fetchTableInfo(md.getChildTableId(),true);

                            DataSet subTable = bizModel.fetchDataSetByName(FieldType.mapPropName(relTableInfo.getTableName()));
                            if(subTable==null){
                                subTable = bizModel.fetchDataSetByName(relTableInfo.getTableName());
                            }
                            if(subTable!=null) {
                                Map<String, Object> ref = new HashMap<>();
                                for (Map.Entry<String, String> rc : md.getReferenceColumns().entrySet()) {
                                    ref.put(rc.getValue(), mainObj.get(rc.getKey()));
                                }
                                GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                    .replaceObjectsAsTabulation(subTable.getData(), ref);
                            }
                        }
                    }
                    return 1;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(bizModel, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int saveObject(String tableId, BizModel bizModel) {
        return inndrsSaveObject(tableId, bizModel, false);
    }

    @Override
    public int mergeObject(String tableId, BizModel bizModel) {
        return inndrsSaveObject(tableId, bizModel, true);
    }

    @Override
    public void deleteObjectBy(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).deleteObjectById(pk));
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).listObjectsByProperties(filter));
        } catch (SQLException | IOException e) {
            throw new ObjectException(filter, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }
}
