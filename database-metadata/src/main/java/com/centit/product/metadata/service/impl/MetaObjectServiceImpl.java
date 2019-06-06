package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.ObjectException;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.support.algorithm.*;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@Service
@Transactional
public class MetaObjectServiceImpl implements MetaObjectService {
    //private Logger logger = LoggerFactory.getLogger(MetaObjectServiceImpl.class);

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    @Autowired
    private MetaTableDao metaTableDao;

    @Autowired
    private MetaRelationDao metaRelationDao;

    private static Map<String, Object> prepareObjectForSave(Map<String, Object> object, MetaTable metaTable){
        for(MetaColumn col :  metaTable.getMdColumns()) {
            Object fieldValue = object.get(col.getPropertyName());
            if(fieldValue != null) {
                switch (col.getJavaType()){
                    case FieldType.DATE:
                    case FieldType.DATETIME:
                        object.put(col.getPropertyName(), DatetimeOpt.castObjectToSqlDate(fieldValue));
                        break;
                    case FieldType.TIMESTAMP:
                        object.put(col.getPropertyName(), DatetimeOpt.castObjectToSqlTimestamp(fieldValue));
                        break;
                    case FieldType.INTEGER:
                    case FieldType.LONG:
                        object.put(col.getPropertyName(), NumberBaseOpt.castObjectToLong(fieldValue));
                        break;
                    case "BigDecimal":
                        object.put(col.getPropertyName(), NumberBaseOpt.castObjectToBigDecimal(fieldValue));
                        break;
                    case FieldType.FLOAT:
                    case FieldType.DOUBLE:
                        object.put(col.getPropertyName(), NumberBaseOpt.castObjectToDouble(fieldValue));
                        break;
                    case FieldType.STRING:
                    case FieldType.TEXT:
                        object.put(col.getPropertyName(), StringBaseOpt.castObjectToString(fieldValue));
                        break;
                    case FieldType.BOOLEAN:
                        object.put(col.getPropertyName(),
                            BooleanBaseOpt.castObjectToBoolean(fieldValue,false)?
                                BooleanBaseOpt.ONE_CHAR_TRUE: BooleanBaseOpt.ONE_CHAR_FALSE);
                        break;
                    default:
                        break;

                }
            }
        }
        return object;
    }


    private static void makeObjectValueByGenerator(Map<String, Object> object, MetaTable metaTable,
                                                    JsonObjectDao sqlDialect)
        throws SQLException, IOException {

        for(MetaColumn col :  metaTable.getMdColumns()) {
            if (StringUtils.equalsAny(col.getAutoCreateRule(), "C", "U", "S", "F")) {
                //只有为空时才创建
                if (object.get(col.getPropertyName()) == null) {
                    switch (col.getAutoCreateRule()) {
                        case "U":
                            object.put(col.getPropertyName(), UuidOpt.getUuidAsString32());
                            break;
                        case "S":
                            //GeneratorTime.READ 读取数据时不能用 SEQUENCE 生成值
                            if (sqlDialect != null) {
                                object.put(col.getPropertyName(),
                                    sqlDialect.getSequenceNextValue(col.getAutoCreateParam()));
                            }
                            break;
                        case "C":
                            object.put(col.getPropertyName(), col.getAutoCreateParam());
                            break;
                        case "F":
                            object.put(col.getPropertyName(),
                                VariableFormula.calculate(col.getAutoCreateParam(), object));
                            break;
                    }
                }
            }
        }
    }

    @Override
    public String getTableId(String databaseCode, String tableName){
        MetaTable metaTable = metaTableDao.getMetaTable(databaseCode, tableName);
        if(metaTable==null){
            return null;
        }
        return metaTable.getTableId();
    }

    @Override
    public MetaTable getTableInfo(String tableId){
        return metaTableDao.getObjectById(tableId);
    }

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

    private Map<String, Object> innerGetObjectById(final Connection conn, final TableInfo tableInfo,final Map<String, Object> pk)
        throws IOException, SQLException {
        GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
        if(dao.checkHasAllPkColumns(pk)){
            return dao.getObjectById(pk);
        } else if( pk.containsKey("flowInstId")) {
            return dao.getObjectByProperties(pk);
        } else {
            throw new ObjectException("表或者视图 " + tableInfo.getTableName()
                +" 缺少对应主键:"+ JSON.toJSONString(pk) );
        }

    }

    @Override
    public Map<String, Object> getObjectById(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> innerGetObjectById(conn, tableInfo, pk));
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    private void fetchObjectRefrences(Connection conn, Map<String, Object> mainObj,
                                      MetaTable tableInfo, int withChildrenDeep) throws SQLException, IOException {
        List<MetaRelation> mds = tableInfo.getMdRelations();
        if(mds!=null) {
            for (MetaRelation md : mds) {
                MetaTable relTableInfo = fetchTableInfo(md.getChildTableId(),true);
                Map<String, Object> ref = md.fetchObjectFk(mainObj);
                JSONArray ja = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                    .listObjectsByProperties(ref);
                MetaTable subTableInfo = fetchTableInfo(md.getChildTableId(),true);
                if(withChildrenDeep >1 && ja != null) {
                    for (Object subObject : ja){
                        if(subObject instanceof Map) {
                            fetchObjectRefrences(conn, (Map<String, Object>) subObject,
                                subTableInfo, withChildrenDeep - 1);
                        }
                    }
                }
                mainObj.put(md.getRelationName(), ja);
            }
        }
    }

    @Override
    public Map<String, Object>  getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = fetchTableInfo(tableId,true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    Map<String, Object> mainObj = innerGetObjectById(conn, tableInfo , pk);
                    if(withChildrenDeep>0) {
                        fetchObjectRefrences(conn, mainObj, tableInfo, withChildrenDeep);
                    }
                    return mainObj;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    /*private void fetchBizModelRefrences(Connection conn, BizModel mainObj,
                                      MetaTable tableInfo, String formTableSql, String whereSql ,int withChildrenDeep) throws SQLException, IOException {
        // 重构bizModel获取方式
    public BizModel getObjectAsBizModel(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = fetchTableInfo(tableId,true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    SimpleBizModel bizModel = new SimpleBizModel(FieldType.mapClassName(tableInfo.getTableName()));
                    Map<String, Object> mainObj =
                        GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).getObjectById(pk);
                    SimpleDataSet dataSet = new SingleRowDataSet(mainObj);
                    dataSet.setDataSetName(tableInfo.getTableName());
                    bizModel.putMainDataSet(dataSet);
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
                            bizModel.putDataSet(md.getRelationName(), subDataSet);
                        }
                    }
                    return bizModel;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }
*/
    @Override
    public int saveObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> {
                    GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                    makeObjectValueByGenerator(object, tableInfo, dao);
                    prepareObjectForSave(object, tableInfo);
                    return dao.saveNewObject(object);
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        prepareObjectForSave(object, tableInfo);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).updateObject(object));
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObjectByProperties(String tableId, final Collection<String> fields, final Map<String, Object> object){
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        prepareObjectForSave(object, tableInfo);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                    return dao.updateObjectsByProperties(fields, object, dao.makePkFieldMap(object));
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObjectsByProperties(String tableId, final Collection<String> fields,
                                  final Map<String, Object> fieldValues,final Map<String, Object> properties){
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        prepareObjectForSave(fieldValues, tableInfo);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo)
                    .updateObjectsByProperties(fields, fieldValues, properties));
        } catch (SQLException | IOException e) {
            throw new ObjectException(fieldValues, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }


    @Override
    public void deleteObject(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        //prepareObjectForSave(pk, tableInfo);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).deleteObjectById(pk));
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    public int innerSaveObject(String tableId, Map<String, Object> mainObj, boolean isMerge) {
        MetaTable tableInfo = fetchTableInfo(tableId, true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            return TransactionHandler.executeQueryInTransaction( JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    if(isMerge) {
                        GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).mergeObject(mainObj);
                    }else {
                        GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                        makeObjectValueByGenerator(mainObj, tableInfo, dao);
                        prepareObjectForSave(mainObj, tableInfo);
                        dao.saveNewObject(mainObj);
                        //GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).saveNewObject(mainObj);
                    }
                    List<MetaRelation> mds = tableInfo.getMdRelations();
                    if(mds!=null) {
                        for (MetaRelation md : mds) {
                            MetaTable relTableInfo = fetchTableInfo(md.getChildTableId(), true);

                            Object subObjects = mainObj.get(md.getRelationName());
                            if (subObjects instanceof List) {
                                List<Map<String, Object>> subTable = (List<Map<String, Object>>)subObjects;
                                GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo);
                                for(Map<String, Object> subObj : subTable){
                                    makeObjectValueByGenerator(subObj, relTableInfo, dao);
                                    prepareObjectForSave(subObj, relTableInfo);
                                }
                                Map<String, Object> ref = new HashMap<>();
                                for (Map.Entry<String, String> rc : md.getReferenceColumns().entrySet()) {
                                    ref.put(rc.getValue(), mainObj.get(rc.getKey()));
                                }
                                dao.replaceObjectsAsTabulation(subTable, ref);
                            }
                        }
                    }
                    return 1;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(mainObj, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int saveObjectWithChildren(String tableId,  Map<String, Object> bizModel) {
        return innerSaveObject(tableId, bizModel, false);
    }

    @Override
    public int updateObjectWithChildren(String tableId, Map<String, Object> bizModel) {
        return innerSaveObject(tableId, bizModel, true);
    }

    @Override
    public void deleteObjectWithChildren(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = fetchTableInfo(tableId, true);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) ->{
                    GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                    Map<String, Object> mainObj = dao.getObjectById(pk);

                    List<MetaRelation> mds = tableInfo.getMdRelations();
                    if(mds!=null) {
                        for (MetaRelation md : mds) {
                            MetaTable relTableInfo = fetchTableInfo(md.getChildTableId(), false);
                            GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                .deleteObjectsByProperties(md.fetchObjectFk(mainObj));
                        }
                    }
                    dao.deleteObjectById(pk);
                    return 1;
                });
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }


    @Override
    public JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            JSONArray ja = TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).listObjectsByProperties(filter));
            return DictionaryMapUtils.mapJsonArray(ja, tableInfo.fetchDictionaryMapColumns());
        } catch (SQLException | IOException e) {
            throw new ObjectException(filter, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, String extFilter, Map<String, Object> params, String [] fields,PageDesc pageDesc) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);

        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            JSONArray ja = TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> {
                    GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);

                    HashSet<String> fieldSet = null ;
                    if(fields !=null && fields.length>0) {
                        fieldSet = new HashSet<>((fields.length + 5) * 3 / 2);
                        fieldSet.addAll(tableInfo.getPkColumns());
                        if(!"0".equals(tableInfo.getWorkFlowOptType())){
                            fieldSet.add(MetaTable.WORKFLOW_INST_ID_PROP);
                            fieldSet.add(MetaTable.WORKFLOW_NODE_INST_ID_PROP);
                        }
                        if(BooleanBaseOpt.castObjectToBoolean(tableInfo.getUpdateCheckTimeStamp(),false)){
                            fieldSet.add(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
                        }
                        Collections.addAll(fieldSet, fields);
                    }
                    Pair<String,String[]> q = (fieldSet == null) ?
                        GeneralJsonObjectDao.buildFieldSqlWithFieldName(tableInfo,null)
                        : GeneralJsonObjectDao.buildPartFieldSqlWithFieldName(tableInfo, fieldSet, null);

                    String filter = GeneralJsonObjectDao.buildFilterSql(tableInfo,null, params.keySet());
                    if(StringUtils.isNotBlank(extFilter)){
                        if(StringUtils.isNotBlank(filter)) {
                            filter = extFilter + " and " + filter;
                        } else {
                            filter = extFilter;
                        }
                    }

                    String sql = "select " + q.getLeft() +" from " +tableInfo.getTableName();
                    if(StringUtils.isNotBlank(filter)) {
                        sql = sql + " where " + filter;
                    }
                    String orderBy = GeneralJsonObjectDao.fetchSelfOrderSql(sql, params);
                    if(StringUtils.isNotBlank(orderBy)){
                        sql = sql + " order by "
                            + QueryUtils.cleanSqlStatement(orderBy);
                    }

                    JSONArray objs = dao.findObjectsByNamedSqlAsJSON(
                        sql, params, q.getRight(), pageDesc.getPageNo(), pageDesc.getPageSize());

                    String sGetCountSql = "select count(1) as totalRows from " + tableInfo.getTableName();
                    if(StringUtils.isNotBlank(filter))
                        sGetCountSql = sGetCountSql + " where " + filter;

                    Object obj = DatabaseAccess.getScalarObjectQuery(conn,
                        sGetCountSql, params);
                    pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(obj));
                    return objs;
                });

            return DictionaryMapUtils.mapJsonArray(ja, tableInfo.fetchDictionaryMapColumns());
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, Map<String, Object> params, String [] fields,PageDesc pageDesc) {
        return pageQueryObjects(tableId, null,  params, fields, pageDesc);
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc) {
        return pageQueryObjects(tableId,null,  params, null, pageDesc);
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, String namedSql, Map<String, Object> params, PageDesc pageDesc) {
        MetaTable tableInfo = fetchTableInfo(tableId, false);
        DatabaseInfo databaseInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        String orderBy = GeneralJsonObjectDao.fetchSelfOrderSql(namedSql, params);
        final String querySql = StringUtils.isBlank(orderBy) ? namedSql
            : QueryUtils.removeOrderBy(namedSql) + " order by "
                + QueryUtils.cleanSqlStatement(orderBy);

        try {
            JSONArray ja = TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> {

                    GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                    JSONArray objs = dao.findObjectsByNamedSqlAsJSON(
                        querySql, params, null, pageDesc.getPageNo(), pageDesc.getPageSize());

                    pageDesc.setTotalRows(
                        NumberBaseOpt.castObjectToInteger(DatabaseAccess.queryTotalRows(conn, querySql, params)));
                    return objs;
                });
            return DictionaryMapUtils.mapJsonArray(ja, tableInfo.fetchDictionaryMapColumns());
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery(paramDriverSql, params);
        return pageQueryObjects(tableId, qap.getQuery(), qap.getParams(), pageDesc);
    }

}
