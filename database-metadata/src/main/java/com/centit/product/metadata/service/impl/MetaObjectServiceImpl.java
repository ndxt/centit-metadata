package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataCache;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.search.document.ObjectDocument;
import com.centit.search.service.Impl.ESIndexer;
import com.centit.support.algorithm.*;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.ObjectTranslate;
import com.centit.support.compiler.Pretreatment;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.transaction.ConnectThreadHolder;
import com.centit.support.database.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@Service
public class MetaObjectServiceImpl implements MetaObjectService {
    //private Logger logger = LoggerFactory.getLogger(MetaObjectServiceImpl.class);

    @Autowired
    private SourceInfoDao sourceInfoDao;

    @Autowired
    private MetaDataCache metaDataCache;

    @Autowired(required = false)
    private ESIndexer esObjectIndexer;

    private static Map<String, Object> prepareObjectForSave(Map<String, Object> object, MetaTable metaTable){
        for(MetaColumn col :  metaTable.getMdColumns()) {
            Object fieldValue = object.get(col.getPropertyName());
            if(fieldValue != null) {
                switch (col.getFieldType()){
                    case FieldType.DATE:
                        /*object.put(col.getPropertyName(), DatetimeOpt.castObjectToSqlDate(fieldValue));
                        break;*/
                    case FieldType.DATETIME:
                    case FieldType.TIMESTAMP:
                        object.put(col.getPropertyName(), DatetimeOpt.castObjectToSqlTimestamp(fieldValue));
                        break;
                    case FieldType.INTEGER:
                    case FieldType.LONG:
                        object.put(col.getPropertyName(), NumberBaseOpt.castObjectToLong(fieldValue));
                        break;
                    case FieldType.MONEY:
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

    private static void makeObjectValueByGenerator(Map<String, Object> object, Map<String, Object> extParams,
                                                   MetaTable metaTable, JsonObjectDao sqlDialect,
                                                   long pkOrder, boolean isGetObject)
        throws SQLException, IOException {

        for(MetaColumn field : metaTable.getMdColumns()) {
            if (StringUtils.equalsAny(field.getAutoCreateRule(), "C", "U", "S", "F","O")) {
                //只有为空时才创建
                if (object.get(field.getPropertyName()) == null) {
                    switch (field.getAutoCreateRule()) {
                        case "U":
                            object.put(field.getPropertyName(), UuidOpt.getUuidAsString32());
                            break;
                        case "S":
                            //GeneratorTime.READ 读取数据时不能用 SEQUENCE 生成值
                            if (sqlDialect != null) {
                                object.put(field.getPropertyName(),
                                    sqlDialect.getSequenceNextValue(field.getAutoCreateParam()));
                            }
                            break;
                        case "C":
                            object.put(field.getPropertyName(), field.getAutoCreateParam());
                            break;
                        case "F":
                            VariableFormula formula = new VariableFormula();
                            formula.addExtendFunc("getSequence", (a) -> {
                                try {
                                    sqlDialect.getSequenceNextValue((String) a[0]);
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                return null;
                            });
                            formula.setFormula(field.getAutoCreateParam());
                            if(extParams != null){
                                Map<String, Object> objectMap = new HashMap<>(extParams.size() + object.size() + 2);
                                objectMap.putAll(extParams);
                                objectMap.putAll(object);
                                formula.setTrans(new ObjectTranslate(objectMap));
                                object.put(field.getPropertyName(),
                                    formula.calcFormula());
                            } else {
                                formula.setTrans(new ObjectTranslate(object));
                                object.put(field.getPropertyName(),
                                    formula.calcFormula());
                            }
                            break;
                        case "O":
                            if (isGetObject) {
                                break;
                            }
                            int pkCount = metaTable.countPkColumn();
                            if(pkCount < 2 || !field.isPrimaryKey()){
                                throw new ObjectException(PersistenceException.ORM_METADATA_EXCEPTION,
                                    "主键生成规则SUB_ORDER必须用于复合主键表中，并且只能用于整型字段！");
                            }
                            StringBuilder sqlBuilder = new StringBuilder("select max(" );
                            sqlBuilder.append(field.getColumnName())
                                .append(" ) as maxOrder from ")
                                .append(metaTable.getTableName())
                                .append(" where ");
                            int pki = 0;
                            Object[] pkValues = new Object[pkCount-1];
                            for(MetaColumn col : metaTable.getColumns()){
                                if(col.isPrimaryKey() &&
                                    ! StringUtils.equals(col.getPropertyName(), field.getPropertyName())){
                                    if(pki>0){
                                        sqlBuilder.append(" and ");
                                    }
                                    sqlBuilder.append(col.getColumnName()).append(" = ?");
                                    pkValues[pki] = object.get(col.getPropertyName());
                                    pki++;
                                }
                            }
                            Long pkSubOrder = NumberBaseOpt.castObjectToLong(
                                DatabaseAccess.fetchScalarObject(
                                    sqlDialect.findObjectsBySql(sqlBuilder.toString(), pkValues)) );
                            object.put(field.getPropertyName(), pkSubOrder == null ? pkOrder : pkSubOrder + pkOrder);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    private SourceInfo fetchDatabaseInfo(String databaseCode){
        return sourceInfoDao.getDatabaseInfoById(databaseCode);
    }

    private Map<String, Object> innerGetObjectById(final Connection conn, final MetaTable tableInfo,final Map<String, Object> pk)
        throws IOException, SQLException {
        GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
        if (pk.size()==0){
            throw new ObjectException(tableInfo.getTableName()+"没有传入主键");
        }
        if(dao.checkHasAllPkColumns(pk)){
            return dao.getObjectById(pk);
        } else if( pk.containsKey(MetaTable.WORKFLOW_INST_ID_PROP)) {
            return dao.getObjectByProperties(pk);
        } else {
            throw new ObjectException("表或者视图 " + tableInfo.getTableName()
                +" 缺少对应主键:"+ JSON.toJSONString(pk) );
        }

    }

    private Map<String, Object> innerGetObjectPartFieldsById(final Connection conn, final MetaTable tableInfo,
                                                             final Map<String, Object> pk, String [] fields)
        throws IOException, SQLException {

        if (pk.size()==0){
            throw new ObjectException(tableInfo.getTableName()+"没有传入主键");
        }
        HashSet<String> fieldSet = collectPartFields(tableInfo, fields);
        Pair<String, TableField[]> q = GeneralJsonObjectDao.buildPartFieldSqlWithFields(tableInfo, fieldSet, null, false);
        String filter;
        if(GeneralJsonObjectDao.checkHasAllPkColumns(tableInfo, pk)){
            filter = GeneralJsonObjectDao.buildFilterSqlByPk(tableInfo, null);
        } else if( pk.containsKey(MetaTable.WORKFLOW_INST_ID_PROP)) {
            filter = GeneralJsonObjectDao.buildFilterSql(tableInfo,null,
                CollectionsOpt.createList(MetaTable.WORKFLOW_INST_ID_PROP,
                    MetaTable.WORKFLOW_NODE_INST_ID_PROP));
        } else {
            throw new ObjectException(tableInfo.getTableName()+"没有传入主键");
        }

        String querySql = "select " + q.getLeft() +
              " from " +tableInfo.getTableName() +
              " where " + filter;
        JSONArray objs = GeneralJsonObjectDao.findObjectsByNamedSql(conn,
            querySql, pk, q.getRight());
        if(objs!=null && objs.size() == 1){
            return (JSONObject)objs.get(0);
        }
        return null;
    }

    @Override
    public Map<String, Object> getObjectById(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            return innerGetObjectById(conn, tableInfo, pk);
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    private void fetchObjectParent(Connection conn, Map<String, Object> mainObj,
                                     MetaRelation md) throws SQLException, IOException {
        MetaTable parentTableInfo = metaDataCache.getTableInfo(md.getParentTableId());
        Map<String, Object> ref = md.fetchParentPk(mainObj);
        // 检查是否是和父表的主键关联
        if (ref != null && GeneralJsonObjectDao.checkHasAllPkColumns(parentTableInfo, ref)) {
            JSONObject ja = GeneralJsonObjectDao.createJsonObjectDao(conn, parentTableInfo)
                .getObjectById(ref);
            mainObj.put(md.getRelationName(),
                DictionaryMapUtils.mapJsonObject(ja,
                    parentTableInfo.fetchDictionaryMapColumns(sourceInfoDao)));
        }
    }

    private void fetchObjectParents(Connection conn, Map<String, Object> mainObj,
                                      MetaTable tableInfo) throws SQLException, IOException {
        List<MetaRelation> mds = tableInfo.getParents();
        if(mds!=null) {
            for (MetaRelation md : mds) {
                if (md.getRelationDetails()!=null) {
                    fetchObjectParent(conn, mainObj, md);
                }
            }
        }
    }

    private void fetchObjectRefrence(Connection conn, Map<String, Object> mainObj,
                                      MetaRelation md) throws SQLException, IOException {
        MetaTable subTableInfo = metaDataCache.getTableInfoWithRelations(md.getChildTableId());
        Map<String, Object> ref = md.fetchChildFk(mainObj);
        if (ref != null) {
            JSONArray ja = DictionaryMapUtils.mapJsonArray(
                GeneralJsonObjectDao.createJsonObjectDao(conn, subTableInfo)
                .listObjectsByProperties(ref), subTableInfo.fetchDictionaryMapColumns(sourceInfoDao));
            mainObj.put(md.getRelationName(), ja);
        }
    }

    private void fetchObjectRefrences(Connection conn, Map<String, Object> mainObj,
                                      MetaTable tableInfo, int withChildrenDeep) throws SQLException, IOException {
        List<MetaRelation> mds = tableInfo.getMdRelations();
        if(mds!=null) {
            for (MetaRelation md : mds) {
                MetaTable subTableInfo = metaDataCache.getTableInfoWithRelations(md.getChildTableId());
                Map<String, Object> ref = md.fetchChildFk(mainObj);
                if(ref!=null) {
                    JSONArray ja = GeneralJsonObjectDao.createJsonObjectDao(conn, subTableInfo)
                        .listObjectsByProperties(ref);
                    ja=DictionaryMapUtils.mapJsonArray(ja, subTableInfo.fetchDictionaryMapColumns(sourceInfoDao));
                    if (withChildrenDeep > 1 && ja != null) {
                        for (Object subObject : ja) {
                            if (subObject instanceof Map) {
                                fetchObjectRefrences(conn, (Map<String, Object>) subObject,
                                    subTableInfo, withChildrenDeep - 1);
                            }
                        }
                    }
                    mainObj.put(md.getRelationName(), ja);
                }
            }
        }
    }

    @Override
    public Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, String [] fields,
                                              String [] parents, String [] children) {
        MetaTable tableInfo = metaDataCache.getTableInfoAll(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            Map<String, Object> mainObj = (fields != null && fields.length > 0) ?
                innerGetObjectPartFieldsById(conn, tableInfo, pk, fields)
                : innerGetObjectById(conn, tableInfo, pk);

            mainObj = DictionaryMapUtils.mapJsonObject(mainObj, tableInfo.fetchDictionaryMapColumns(sourceInfoDao));
            return fetchObjectParentAndChildren(tableInfo, mainObj, parents, children);
        }catch (SQLException | IOException e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }

    }

    @Override
    public Map<String, Object> fetchObjectParentAndChildren(MetaTable tableInfo, Map<String, Object> mainObj,
                                                     String [] parents, String [] children){
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            if(parents != null && parents.length>0){
                List<MetaRelation> mds = tableInfo.getParents();
                if(mds!=null) {
                    for (MetaRelation md : mds) {
                        if (StringUtils.equalsAny(md.getReferenceName(), parents) && md.getRelationDetails()!=null) {
                            fetchObjectParent(conn, mainObj, md);
                        }
                    }
                }
            }
            List<MetaRelation> mds = tableInfo.getMdRelations();
            if(mds!=null) {
                for (MetaRelation md : mds) {
                    if (StringUtils.equalsAny(md.getReferenceName(), children) && md.getRelationDetails()!=null) {
                        fetchObjectRefrence(conn, mainObj, md);
                    }
                }
            }
            return mainObj;
        } catch (SQLException | IOException e) {
            throw new ObjectException(mainObj, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object>  getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfoAll(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            Map<String, Object> mainObj = innerGetObjectById(conn, tableInfo , pk);
            mainObj = DictionaryMapUtils.mapJsonObject(mainObj, tableInfo.fetchDictionaryMapColumns(sourceInfoDao));
            if(withChildrenDeep>0 && mainObj!=null) {
                fetchObjectRefrences(conn, mainObj, tableInfo, withChildrenDeep);
            }
            fetchObjectParents(conn, mainObj, tableInfo);
            return mainObj;
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object> makeNewObject(String tableId, Map<String, Object> extParams){
        MetaTable tableInfo = metaDataCache.getTableInfoWithParents(tableId);
        JSONObject objectMap = new JSONObject();
        if(extParams!=null && !extParams.isEmpty()){
            for(MetaColumn col : tableInfo.getColumns()){
                Object colValue = extParams.get(col.getPropertyName());
                if(colValue != null){
                    objectMap.put(col.getPropertyName(), colValue);
                }
            }
        }
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            makeObjectValueByGenerator(objectMap, extParams, tableInfo, dao, 1l,true);
            fetchObjectParents(conn, objectMap, tableInfo);
            return objectMap;
        } catch (SQLException | IOException e) {
            throw new ObjectException(objectMap, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object> makeNewObject(String tableId){
        return makeNewObject(tableId,null);
    }

    @Override
    public int saveObject(String tableId, Map<String, Object> object, Map<String, Object> extParams) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            makeObjectValueByGenerator(object, extParams, tableInfo, dao, 1l,false);
            prepareObjectForSave(object, tableInfo);
            return dao.saveNewObject(object);
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int saveObject(String tableId, Map<String, Object> object) {
        return saveObject(tableId,object, null);
    }

    @Override
    public int updateObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        prepareObjectForSave(object, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            return GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).updateObject(object);
        } catch (SQLException e) {
            throw new ObjectException(object, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObjectFields(String tableId, final Collection<String> fields, final Map<String, Object> object){
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        prepareObjectForSave(object, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            return dao.updateObjectsByProperties(fields, object, dao.makePkFieldMap(object));
        } catch (SQLException e) {
            throw new ObjectException(object, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObjectsByProperties(String tableId, final Collection<String> fields,
                                  final Map<String, Object> fieldValues,final Map<String, Object> filterProperties){
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        prepareObjectForSave(fieldValues, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            return GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo)
                    .updateObjectsByProperties(fields, fieldValues, filterProperties);
        } catch (SQLException e) {
            throw new ObjectException(fieldValues, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObjectsByProperties(String tableId,
                                         final Map<String, Object> fieldValues,
                                         final Map<String, Object> filterProperties) {
        return updateObjectsByProperties(tableId, fieldValues.keySet(),
                fieldValues, filterProperties);
    }
    private void deleteFulltextIndex(Map<String, Object> obj, String tableId) {
        MetaTable metaTable = metaDataCache.getTableInfo(tableId);
        if (esObjectIndexer!=null && metaTable != null &&
            ("T".equals(metaTable.getFulltextSearch())
                // 用json格式保存在大字段中的内容不能用sql检索，必须用全文检索
                || "C".equals(metaTable.getTableType()))) {
            try {
                esObjectIndexer.deleteDocument(
                    mapObjectToDocument(obj, metaTable));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public void deleteObject(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        //prepareObjectForSave(pk, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).deleteObjectById(pk);
            deleteFulltextIndex(pk,tableId);
        } catch (SQLException e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }
    private void checkUpdateTimeStamp(Map<String, Object> dbObject, Map<String, Object> object) {
        Object oldDate = dbObject.get(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
        Object newDate = object.get(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
        if (newDate==null || oldDate==null) {
            return;
        }
        if (!DatetimeOpt.equalOnSecond(DatetimeOpt.castObjectToDate(oldDate), DatetimeOpt.castObjectToDate(newDate))) {
            throw new ObjectException(CollectionsOpt.createHashMap(
                "yourTimeStamp", newDate, "databaseTimeStamp", oldDate),
                PersistenceException.DATABASE_OUT_SYNC_EXCEPTION, "更新数据对象时，数据版本不同步。");
        }

        object.put(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP, DatetimeOpt.currentSqlDate());
    }
    private ObjectDocument mapObjectToDocument(Map<String, Object> object, MetaTable metaTable) {
        ObjectDocument doc = new ObjectDocument();
        doc.setOsId(metaTable.getDatabaseCode());
        doc.setOptId(metaTable.getTableId());
        //Map<String, Object> pkMap = metaTable.fetchObjectPkAsId(object);
        doc.setOptTag(metaTable.fetchObjectPkAsId(object));
        doc.contentObject(object);//.setContent(JSON.toJSONString(object));
        doc.setTitle(Pretreatment.mapTemplateString(metaTable.getObjectTitle(), object));
        doc.setUserCode((String) object.get("userCode"));
        doc.setUnitCode((String) object.get("unitCode"));
        return doc;
    }
    private void saveFulltextIndex(Map<String, Object> obj, MetaTable metaTable) {
        //MetaTable metaTable = metaDataCache.getTableInfo(tableId);
        if (esObjectIndexer!=null && metaTable != null &&
            ("T".equals(metaTable.getFulltextSearch())
                // 用json格式保存在大字段中的内容不能用sql检索，必须用全文检索
                || "C".equals(metaTable.getTableType()))) {
            try {
                esObjectIndexer.saveNewDocument(
                    mapObjectToDocument(obj, metaTable));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void updataFulltextIndex(Map<String, Object> obj, MetaTable metaTable) {
        //MetaTable metaTable = metaDataCache.getTableInfo(tableId);
        if (esObjectIndexer!=null && metaTable != null &&
            ("T".equals(metaTable.getFulltextSearch())
                // 用json格式保存在大字段中的内容不能用sql检索，必须用全文检索
                || "C".equals(metaTable.getTableType()))) {
            try {
                Map<String, Object> dbObject =
                    getObjectWithChildren(metaTable.getTableId(), obj, 1);
                esObjectIndexer.mergeDocument(
                    mapObjectToDocument(dbObject, metaTable));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public int innerSaveObject(String tableId, Map<String, Object> mainObj,Map<String, Object> extParams, boolean isUpdate) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        if ("C".equals(tableInfo.getTableType())) {
            mainObj = mapDtoToPo(mainObj);
        }
        if (tableInfo.isUpdateCheckTimeStamp()) {
            if (isUpdate){
                Map<String, Object> dbObject=getObjectWithChildren(tableId, mainObj, 1);
                checkUpdateTimeStamp(dbObject, mainObj);
            }else {
                mainObj.put(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP, DatetimeOpt.currentSqlDate());
            }
        }
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect( DataSourceDescription.valueOf(sourceInfo));
            if(isUpdate) {
                prepareObjectForSave(mainObj, tableInfo);
                GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).updateObject(mainObj);
            }else {
                GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                makeObjectValueByGenerator(mainObj, extParams, tableInfo, dao, 1l,false);
                prepareObjectForSave(mainObj, tableInfo);
                dao.saveNewObject(mainObj);
                //GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).saveNewObject(mainObj);
            }
            List<MetaRelation> mds = tableInfo.getMdRelations();
            if(mds!=null) {
                for (MetaRelation md : mds) {
                    MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                    if ("T".equals(relTableInfo.getTableType())) {
                        Object subObjects = mainObj.get(md.getRelationName());
                        if (subObjects instanceof List) {
                            List<Map<String, Object>> subTable = (List<Map<String, Object>>) subObjects;
                            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo);
                            Map<String, Object> ref = md.fetchChildFk(mainObj);
                            long order = 1l;
                            for (Map<String, Object> subObj : subTable) {
                                subObj.putAll(ref);
                                makeObjectValueByGenerator(subObj, extParams, relTableInfo, dao, order, false);
                                order++;
                                prepareObjectForSave(subObj, relTableInfo);
                            }
                            dao.replaceObjectsAsTabulation(subTable, ref);
                        }
                    }
                }
            }
            if(isUpdate){
                updataFulltextIndex(mainObj,tableInfo);
            }
            else{
                saveFulltextIndex(mainObj, tableInfo);
            }
            return 1;
        } catch (SQLException | IOException e) {
            throw new ObjectException(mainObj, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int saveObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams) {
        return innerSaveObject(tableId, object, extParams, false);
    }

    @Override
    public int saveObjectWithChildren(String tableId, Map<String, Object> object) {
        return innerSaveObject(tableId, object, null,false);
    }

    @Override
    public int updateObjectWithChildren(String tableId, Map<String, Object> object) {
        return innerSaveObject(tableId, object, null,true);
    }

    @Override
    public void deleteObjectWithChildren(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            Map<String, Object> mainObj = dao.getObjectById(pk);

            List<MetaRelation> mds = tableInfo.getMdRelations();
            if(mds!=null) {
                for (MetaRelation md : mds) {
                    MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                    if ("T".equals(relTableInfo.getTableType())) {
                        GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                            .deleteObjectsByProperties(md.fetchChildFk(mainObj));
                    }
                }
            }
            dao.deleteObjectById(pk);
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }


    @Override
    public JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            JSONArray ja = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).listObjectsByProperties(filter);
            return DictionaryMapUtils.mapJsonArray(ja, tableInfo.fetchDictionaryMapColumns(sourceInfoDao));
        } catch (SQLException | IOException e) {
            throw new ObjectException(filter, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    private HashSet<String> collectPartFields(MetaTable tableInfo, String [] fields){
        HashSet<String> fieldSet = new HashSet<>((fields.length + 5) * 3 / 2);
        for(TableField pkField : tableInfo.getPkFields()) {
            fieldSet.add(pkField.getPropertyName());
        }
        if(!"0".equals(tableInfo.getWorkFlowOptType())){
            fieldSet.add(MetaTable.WORKFLOW_INST_ID_PROP);
            fieldSet.add(MetaTable.WORKFLOW_NODE_INST_ID_PROP);
        }
        if(BooleanBaseOpt.castObjectToBoolean(tableInfo.getUpdateCheckTimeStamp(),false)){
            fieldSet.add(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
        }
        Collections.addAll(fieldSet, fields);
        return fieldSet;
    }
    private Map<String, Object> mapPoToDto(Map<String, Object> po) {
        Object obj = po.get(MetaTable.OBJECT_AS_CLOB_PROP);
        if (/*obj!=null && */obj instanceof Map) {
            Map<String, Object> dto = (Map<String, Object>) obj;
            for (Map.Entry<String, Object> ent : po.entrySet()) {
                if (!MetaTable.OBJECT_AS_CLOB_PROP.equals(ent.getKey()) && ent.getValue() != null) {
                    dto.put(ent.getKey(), ent.getValue());
                }
            }
            return dto;
        }
        return po;
    }
    private JSONArray mapListPoToDto(JSONArray ja ) {
        if(ja == null){
            return null;
        }

        JSONArray jsonArray = new JSONArray(ja.size());
        for (Object json : ja) {
            if(json instanceof Map) {
                jsonArray.add(mapPoToDto((Map<String, Object>) json));
            } else {
                jsonArray.add(json);
            }
        }
        return jsonArray;
    }
    private Map<String, Object> mapDtoToPo(Map<String, Object> dto) {
        Map<String, Object> po = new HashMap<>(dto);
        //Map<String, Object> po = dto;
        po.remove(MetaTable.OBJECT_AS_CLOB_PROP);
        String jsonString = JSON.toJSONString(po);
        po.put(MetaTable.OBJECT_AS_CLOB_PROP, jsonString);
        return po;
    }
    @Override
    public JSONArray pageQueryObjects(String tableId, String extFilter,
                                      Map<String, Object> params, String [] fields,
                                      PageDesc pageDesc) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);

        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            //GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            HashSet<String> fieldSet = null ;
            if(fields !=null && fields.length>0) {
                fieldSet = collectPartFields(tableInfo, fields);
            }
            Pair<String, TableField[]> q = (fieldSet == null) ?
                GeneralJsonObjectDao.buildFieldSqlWithFields(tableInfo, null, true)
                : GeneralJsonObjectDao.buildPartFieldSqlWithFields(tableInfo, fieldSet, null, false);

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

            String querySql = QueryUtils.buildLimitQuerySQL(sql,
                pageDesc.getRowStart(), pageDesc.getPageSize(),false,
                sourceInfo.getDBType());

            JSONArray objs = GeneralJsonObjectDao.findObjectsByNamedSql(conn,
                querySql, params, q.getRight());//, pageDesc.getPageNo(), pageDesc.getPageSize());

            String sGetCountSql = "select count(1) as totalRows from " + tableInfo.getTableName();
            if(StringUtils.isNotBlank(filter)) {
                sGetCountSql = sGetCountSql + " where " + filter;
            }

            Object obj = DatabaseAccess.getScalarObjectQuery(conn,
                sGetCountSql, params);
            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(obj));

            JSONArray ja = DictionaryMapUtils.mapJsonArray(objs, tableInfo.fetchDictionaryMapColumns(sourceInfoDao));
            if ("C".equals(tableInfo.getTableType())) {
                ja=mapListPoToDto(ja);
            }
            return ja;

        } catch (SQLException | IOException e) {
            throw new ObjectException(params, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
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
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        String orderBy = GeneralJsonObjectDao.fetchSelfOrderSql(namedSql, params);
        final String querySql = StringUtils.isBlank(orderBy) ? namedSql
            : QueryUtils.removeOrderBy(namedSql) + " order by "
                + QueryUtils.cleanSqlStatement(orderBy);
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(DataSourceDescription.valueOf(sourceInfo));
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);

            JSONArray objs = dao.findObjectsByNamedSqlAsJSON(
                querySql, params, null, pageDesc.getPageNo(), pageDesc.getPageSize());

            pageDesc.setTotalRows(
                NumberBaseOpt.castObjectToInteger(DatabaseAccess.queryTotalRows(conn, querySql, params)));
            return DictionaryMapUtils.mapJsonArray(objs, tableInfo.fetchDictionaryMapColumns(sourceInfoDao));
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery(paramDriverSql, params);
        return pageQueryObjects(tableId, qap.getQuery(), qap.getParams(), pageDesc);
    }

}
