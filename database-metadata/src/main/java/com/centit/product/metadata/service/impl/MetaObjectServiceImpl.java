package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.DictionaryMapColumn;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.product.adapter.po.*;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.service.MetaDataCache;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.product.metadata.transaction.AbstractSourceConnectThreadHolder;
import com.centit.product.metadata.utils.DataCheckResult;
import com.centit.support.algorithm.*;
import com.centit.support.common.CachedObject;
import com.centit.support.common.ICachedObject;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.ObjectTranslate;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.*;
import com.centit.support.security.Md5Encoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class MetaObjectServiceImpl implements MetaObjectService {
    //private Logger logger = LoggerFactory.getLogger(MetaObjectServiceImpl.class);

    @Autowired
    private SourceInfoDao sourceInfoDao;

    @Autowired
    private MetaDataCache metaDataCache;

    @Autowired
    private DataCheckRuleDao dataCheckRuleDao;

    private static Map<String, Object> prepareObjectForSave(Map<String, Object> object, MetaTable metaTable) {
        for (MetaColumn col : metaTable.getMdColumns()) {
            Object fieldValue = object.get(col.getPropertyName());
            if (fieldValue != null) {
                switch (col.getFieldType()) {
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
                            BooleanBaseOpt.castObjectToBoolean(fieldValue, false) ?
                                BooleanBaseOpt.ONE_CHAR_TRUE : BooleanBaseOpt.ONE_CHAR_FALSE);
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

        for (MetaColumn field : metaTable.getMdColumns()) {
            if (StringUtils.equalsAny(field.getAutoCreateRule(), "C", "U", "S", "F", "O")) {
                //只有为空时才创建
                if (object.get(field.getPropertyName()) == null) {
                    switch (field.getAutoCreateRule()) {
                        case "U": // uuid
                            object.put(field.getPropertyName(), UuidOpt.getUuidAsString32());
                            break;
                        case "S": // sequence
                            //GeneratorTime.READ 读取数据时不能用 SEQUENCE 生成值
                            if (sqlDialect != null) {
                                object.put(field.getPropertyName(),
                                    sqlDialect.getSequenceNextValue(field.getAutoCreateParam()));
                            }
                            break;
                        case "C": // const
                            object.put(field.getPropertyName(), field.getAutoCreateParam());
                            break;
                        case "F": // formula
                            VariableFormula formula = new VariableFormula();
                            formula.addExtendFunc("getSequence", (a) -> {
                                try {
                                    sqlDialect.getSequenceNextValue((String) a[0]);
                                } catch (SQLException | IOException e) {
                                    e.printStackTrace();
                                }
                                return null;
                            });
                            formula.setFormula(field.getAutoCreateParam());
                            if (extParams != null) {
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
                        case "O": // order
                            if (isGetObject) {
                                break;
                            }
                            int pkCount = metaTable.countPkColumn();
                            if (pkCount < 2 || !field.isPrimaryKey()) {
                                throw new ObjectException(PersistenceException.ORM_METADATA_EXCEPTION,
                                    "主键生成规则SUB_ORDER必须用于复合主键表中，并且只能用于整型字段！");
                            }
                            StringBuilder sqlBuilder = new StringBuilder("select max(");
                            sqlBuilder.append(field.getColumnName())
                                .append(" ) as maxOrder from ")
                                .append(metaTable.getTableName())
                                .append(" where ");
                            int pki = 0;
                            Object[] pkValues = new Object[pkCount - 1];
                            for (MetaColumn col : metaTable.getColumns()) {
                                if (col.isPrimaryKey() &&
                                    !StringUtils.equals(col.getPropertyName(), field.getPropertyName())) {
                                    if (pki > 0) {
                                        sqlBuilder.append(" and ");
                                    }
                                    sqlBuilder.append(col.getColumnName()).append(" = ?");
                                    pkValues[pki] = object.get(col.getPropertyName());
                                    pki++;
                                }
                            }
                            Long pkSubOrder = NumberBaseOpt.castObjectToLong(
                                DatabaseAccess.fetchScalarObject(
                                    sqlDialect.findObjectsBySql(sqlBuilder.toString(), pkValues)));
                            object.put(field.getPropertyName(), pkSubOrder == null ? pkOrder : pkSubOrder + pkOrder);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    private SourceInfo fetchDatabaseInfo(String databaseCode) {
        return sourceInfoDao.getDatabaseInfoById(databaseCode);
    }

    private Map<String, Object> innerGetObjectById(final Connection conn, final MetaTable tableInfo, final Map<String, Object> pk)
        throws IOException, SQLException {
        GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
        if (pk.size() == 0) {
            throw new ObjectException(tableInfo.getTableName() + "没有传入主键");
        }
        if (dao.checkHasAllPkColumns(pk)) {
            return dao.getObjectById(pk);
        } else if (pk.containsKey(MetaTable.WORKFLOW_INST_ID_PROP)) {
            return dao.getObjectByProperties(pk);
        } else {
            throw new ObjectException("表或者视图 " + tableInfo.getTableName()
                + " 缺少对应主键:" + JSON.toJSONString(pk));
        }

    }

    private Map<String, Object> innerGetObjectPartFieldsById(final Connection conn, final MetaTable tableInfo,
                                                             final Map<String, Object> pk, String[] fields)
        throws IOException, SQLException {

        if (pk.size() == 0) {
            throw new ObjectException(tableInfo.getTableName() + "没有传入主键");
        }
        HashSet<String> fieldSet = collectPartFields(tableInfo, fields);
        Pair<String, TableField[]> q = GeneralJsonObjectDao.buildPartFieldSqlWithFields(tableInfo, fieldSet, null, false);
        String filter;
        if (GeneralJsonObjectDao.checkHasAllPkColumns(tableInfo, pk)) {
            filter = GeneralJsonObjectDao.buildFilterSqlByPk(tableInfo, null);
        } else if (pk.containsKey(MetaTable.WORKFLOW_INST_ID_PROP)) {
            filter = GeneralJsonObjectDao.buildFilterSql(tableInfo, null,
                CollectionsOpt.createHashMap(MetaTable.WORKFLOW_INST_ID_PROP,
                    pk.get(MetaTable.WORKFLOW_INST_ID_PROP),
                    MetaTable.WORKFLOW_NODE_INST_ID_PROP,
                    pk.get(MetaTable.WORKFLOW_NODE_INST_ID_PROP)));
        } else {
            throw new ObjectException(tableInfo.getTableName() + "没有传入主键");
        }

        String querySql = "select " + q.getLeft() +
            " from " + tableInfo.getTableName() +
            " where " + filter;
        JSONArray objs = GeneralJsonObjectDao.findObjectsByNamedSql(conn,
            querySql, pk, q.getRight());
        if (objs != null && objs.size() == 1) {
            return (JSONObject) objs.get(0);
        }
        return null;
    }

    @Override
    public Map<String, Object> getObjectById(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            return innerGetObjectById(conn, tableInfo, pk);
        } catch (Exception e) {
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
                    this.fetchDictionaryMapColumns(sourceInfoDao, parentTableInfo)));
        }
    }

    private void fetchObjectParents(Connection conn, Map<String, Object> mainObj,
                                    MetaTable tableInfo) throws SQLException, IOException {
        List<MetaRelation> mds = tableInfo.getParents();
        if (mds != null) {
            for (MetaRelation md : mds) {
                if (md.getRelationDetails() != null) {
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
                    .listObjectsByProperties(ref), this.fetchDictionaryMapColumns(sourceInfoDao, subTableInfo));
            mainObj.put(md.getRelationName(), ja);
        }
    }

    private void fetchObjectRefrences(Connection conn, Map<String, Object> mainObj,
                                      MetaTable tableInfo, int withChildrenDeep) throws SQLException, IOException {
        List<MetaRelation> mds = tableInfo.getMdRelations();
        if (mds != null) {
            for (MetaRelation md : mds) {
                MetaTable subTableInfo = metaDataCache.getTableInfoWithRelations(md.getChildTableId());
                Map<String, Object> ref = md.fetchChildFk(mainObj);
                if (ref != null) {
                    JSONArray ja = GeneralJsonObjectDao.createJsonObjectDao(conn, subTableInfo)
                        .listObjectsByProperties(ref);
                    ja = DictionaryMapUtils.mapJsonArray(ja, fetchDictionaryMapColumns(sourceInfoDao, subTableInfo));
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
    public Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, String[] fields,
                                                     String[] parents, String[] children) {
        MetaTable tableInfo = metaDataCache.getTableInfoAll(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Map<String, Object> mainObj;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            mainObj = (fields != null && fields.length > 0) ?
                innerGetObjectPartFieldsById(conn, tableInfo, pk, fields)
                : innerGetObjectById(conn, tableInfo, pk);
            mainObj = DictionaryMapUtils.mapJsonObject(mainObj, this.fetchDictionaryMapColumns(sourceInfoDao, tableInfo));
            return fetchObjectParentAndChildren(tableInfo, mainObj, parents, children);
        } catch (Exception e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object> fetchObjectParentAndChildren(MetaTable tableInfo, Map<String, Object> mainObj,
                                                            String[] parents, String[] children) {
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            if (parents != null && parents.length > 0) {
                List<MetaRelation> mds = tableInfo.getParents();
                if (mds != null) {
                    for (MetaRelation md : mds) {
                        if (StringUtils.equalsAny(md.getReferenceName(), parents) && md.getRelationDetails() != null) {
                            fetchObjectParent(conn, mainObj, md);
                        }
                    }
                }
            }
            List<MetaRelation> mds = tableInfo.getMdRelations();
            if (mds != null) {
                for (MetaRelation md : mds) {
                    if (StringUtils.equalsAny(md.getReferenceName(), children) && md.getRelationDetails() != null) {
                        fetchObjectRefrence(conn, mainObj, md);
                    }
                }
            }
            return mainObj;
        } catch (Exception e) {
            throw new ObjectException(mainObj, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfoAll(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Map<String, Object> mainObj;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            mainObj = innerGetObjectById(conn, tableInfo, pk);
            mainObj = DictionaryMapUtils.mapJsonObject(mainObj, this.fetchDictionaryMapColumns(sourceInfoDao, tableInfo));
            if (withChildrenDeep > 0 && mainObj != null) {
                fetchObjectRefrences(conn, mainObj, tableInfo, withChildrenDeep);
            }
            fetchObjectParents(conn, mainObj, tableInfo);
            return mainObj;
        } catch (Exception e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object> makeNewObject(String tableId, Map<String, Object> extParams) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithParents(tableId);
        JSONObject objectMap = new JSONObject();
        if (extParams != null && !extParams.isEmpty()) {
            for (MetaColumn col : tableInfo.getColumns()) {
                Object colValue = extParams.get(col.getPropertyName());
                if (colValue != null) {
                    objectMap.put(col.getPropertyName(), colValue);
                }
            }
        }
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            makeObjectValueByGenerator(objectMap, extParams, tableInfo, dao, 1l, true);

            fetchObjectParents(conn, objectMap, tableInfo);
            return objectMap;
        } catch (Exception e) {
            throw new ObjectException(objectMap, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public Map<String, Object> makeNewObject(String tableId) {
        return makeNewObject(tableId, null);
    }

    //校验字段是否符合规则约束
    private void checkFieldRule(MetaTable tableInfo, Map<String, Object> object) {
        List<MetaColumn> columns = tableInfo.getColumns();
        //过滤出需要校验的字段
        List<MetaColumn> checkFieldList = columns.stream().filter(metaColumn ->
            StringUtils.isNotBlank(metaColumn.getCheckRuleId())).collect(Collectors.toList());
        DataCheckResult result = DataCheckResult.create();
        for (MetaColumn metaColumn : checkFieldList) {
            String columnName = metaColumn.getColumnName();
            DataCheckRule dataCheckRule = dataCheckRuleDao.getObjectById(metaColumn.getCheckRuleId());
            Map<String, Object> checkParam = CollectionsOpt.objectToMap(metaColumn.getCheckRuleParams());
            Map<String, String> param = new HashMap<>();
            param.put(DataCheckRule.CHECK_VALUE_TAG, columnName);
            checkParam.forEach((key, value) -> {
                Map<String, Object> checkRouleInfo = CollectionsOpt.objectToMap(value);
                String checkValue = StringBaseOpt.castObjectToString(checkRouleInfo.get("value"));
                if (StringUtils.isNotBlank(checkValue)) {
                    param.put(key, checkValue);
                }
            });
            result.checkData(object, dataCheckRule, param);
        }
        if (!result.getResult()) {
            throw new ObjectException(StringBaseOpt.castObjectToString(result.getErrorMsgs()));
        }
    }

    @Override
    public int updateObject(String tableId, Map<String, Object> object) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        checkFieldRule(tableInfo, object);
        prepareObjectForSave(object, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            return GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).updateObject(object);
        } catch (Exception e) {
            throw new ObjectException(object, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }


    @Override
    public int updateObjectFields(String tableId, final Collection<String> fields, final Map<String, Object> object) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        checkFieldRule(tableInfo, object);
        prepareObjectForSave(object, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            GeneralJsonObjectDao dao;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            return dao.updateObjectsByProperties(fields, object, dao.makePkFieldMap(object));
        } catch (Exception e) {
            throw new ObjectException(object, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int deleteObjectsByProperties(String tableId,
                                         final Map<String, Object> filterProperties) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            return GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo)
                .deleteObjectsByProperties(filterProperties);
        } catch (Exception e) {
            throw new ObjectException(filterProperties, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int updateObjectsByProperties(String tableId, final Collection<String> fields,
                                         final Map<String, Object> fieldValues, final Map<String, Object> filterProperties) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        checkFieldRule(tableInfo, fieldValues);
        prepareObjectForSave(fieldValues, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            return GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo)
                .updateObjectsByProperties(fields, fieldValues, filterProperties);
        } catch (Exception e) {
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


    @Override
    public void deleteObject(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        //prepareObjectForSave(pk, tableInfo);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).deleteObjectById(pk);
        } catch (Exception e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    private void checkUpdateTimeStamp(Map<String, Object> dbObject, Map<String, Object> object) {
        Object oldDate = dbObject.get(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
        Object newDate = object.get(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
        if (newDate == null || oldDate == null) {
            return;
        }
        if (!DatetimeOpt.equalOnSecond(DatetimeOpt.castObjectToDate(oldDate), DatetimeOpt.castObjectToDate(newDate))) {
            throw new ObjectException(CollectionsOpt.createHashMap(
                "yourTimeStamp", newDate, "databaseTimeStamp", oldDate),
                PersistenceException.DATABASE_OUT_SYNC_EXCEPTION, "更新数据对象时，数据版本不同步。");
        }

        object.put(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP, DatetimeOpt.currentSqlDate());
    }


    @Override
    public int saveObject(String tableId, Map<String, Object> object) {
        return innerSaveObject(tableId, object, null, false, 0);
    }

    @Override
    public int saveObject(String tableId, Map<String, Object> object, Map<String, Object> extParams) {
        return innerSaveObject(tableId, object, extParams, false, 0);
    }

    public int replaceObjectsAsTabulation(GeneralJsonObjectDao dao,  MetaTable relTableInfo,
                                          final List<Map<String, Object>> newObjects, Map<String, Object> extParams,
                                          final Map<String, Object> properties, int withChildrenDeep)
        throws SQLException, IOException {
        List<Map<String, Object>> dbObjects =  (List) dao.listObjectsByProperties(properties);
        Triple<List<Map<String, Object>>, List<Pair<Map<String, Object>, Map<String, Object>>>, List<Map<String, Object>>>
            comRes =
            CollectionsOpt.compareTwoList(dbObjects, newObjects, new GeneralJsonObjectDao.JSONObjectComparator(relTableInfo));

        int resN = 0;
        if (comRes.getLeft() != null) {
            for (Map<String, Object> obj : comRes.getLeft()) {
                resN += innerSaveObject(relTableInfo.getTableId(), obj, extParams, false, withChildrenDeep);
            }
        }
        if (comRes.getRight() != null) {
            for (Map<String, Object> obj : comRes.getRight()) {
                resN ++;
                deleteObjectWithChildren(relTableInfo.getTableId(), obj, withChildrenDeep);
            }
        }
        if (comRes.getMiddle() != null) {
            for (Pair<Map<String, Object>, Map<String, Object>> pobj : comRes.getMiddle()) {
                //对比减少不必要的更新
                if(dao.checkNeedUpdate(pobj.getLeft(), pobj.getRight())) {
                    resN += innerSaveObject(relTableInfo.getTableId(), pobj.getRight(), extParams, true, withChildrenDeep);
                }
            }
        }
        return resN;
    }

    private int innerSaveObject(String tableId, Map<String, Object> mainObj, Map<String, Object> extParams, boolean isUpdate, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        ///todo 添加规则判段
        checkFieldRule(tableInfo, mainObj);
        if (tableInfo.isUpdateCheckTimeStamp()) {
            if (isUpdate) {
                Map<String, Object> dbObject = getObjectWithChildren(tableId, mainObj, 1);
                checkUpdateTimeStamp(dbObject, mainObj);
            } else {
                mainObj.put(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP, DatetimeOpt.currentSqlDate());
            }
        }
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            if (isUpdate) {
                prepareObjectForSave(mainObj, tableInfo);
                GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).updateObject(mainObj);
            } else {
                GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                makeObjectValueByGenerator(mainObj, extParams, tableInfo, dao, 1l, false);
                prepareObjectForSave(mainObj, tableInfo);
                if(tableInfo.hasGeneratedKeys()){
                    Map<String, Object> ids = dao.saveNewObjectAndFetchGeneratedKeys(mainObj);
                    //写回主键值 20221214
                    if(ids !=null && !ids.isEmpty()) {
                        MetaColumn column = tableInfo.fetchGeneratedKey();
                        if (column != null){
                            mainObj.put(column.getPropertyName(), ids.values().iterator().next());
                        }
                    }
                } else {
                    dao.saveNewObject(mainObj);
                }
            }
            if(withChildrenDeep<1){
                return 1;
            }

            List<MetaRelation> mds = tableInfo.getMdRelations();
            if (mds != null) {
                for (MetaRelation md : mds) {
                    MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                    if ("T".equals(relTableInfo.getTableType())) {
                        Object subObjects = mainObj.get(md.getRelationName());
                        if (subObjects instanceof List) {
                            List<Map<String, Object>> subTable = (List<Map<String, Object>>) subObjects;
                            //List<MetaRelation> mdchilds = relTableInfo.getMdRelations();
                            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo);
                            Map<String, Object> ref = md.fetchChildFk(mainObj);

                            for (Map<String, Object> subObj : subTable) {
                                subObj.putAll(ref);
                            }
                            this.replaceObjectsAsTabulation(dao, relTableInfo, subTable, extParams,
                                ref, withChildrenDeep - 1);
                        }
                    }
                }
            }
            return 1;
        } catch (Exception e) {
            throw new ObjectException(mainObj, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int saveObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep) {
        return innerSaveObject(tableId, object, extParams, false, withChildrenDeep);
    }

    @Override
    public int saveObjectWithChildren(String tableId, Map<String, Object> object, int withChildrenDeep) {
        return innerSaveObject(tableId, object, null, false, withChildrenDeep);
    }

    @Override
    public int updateObjectWithChildren(String tableId, Map<String, Object> object, int withChildrenDeep) {
        return innerSaveObject(tableId, object, null, true, withChildrenDeep);
    }

    @Override
    public void deleteObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            GeneralJsonObjectDao dao;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            Map<String, Object> mainObj = dao.getObjectById(pk);
            if (null == mainObj || mainObj.size() == 0) {
                return;
            }
            List<MetaRelation> mds = tableInfo.getMdRelations();
            if (mds != null) {
                for (MetaRelation md : mds) {
                    MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                    if ("T".equals(relTableInfo.getTableType())) {
                        List<MetaRelation> mdChilds = relTableInfo.getMdRelations();
                        if (mdChilds != null && withChildrenDeep > 1) {
                            deleteObjectWithChildren(relTableInfo.getTableId(), md.fetchChildFk(mainObj), withChildrenDeep - 1);
                        } else {
                            GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                .deleteObjectsByProperties(md.fetchChildFk(mainObj));
                        }
                    }
                }
            }
            dao.deleteObjectById(pk);
        } catch (Exception e) {
            throw new ObjectException(pk, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int mergeObjectWithChildren(String tableId, Map<String, Object> object, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        Map<String, Object> dbObjectPk = tableInfo.fetchObjectPk(object);
        Map<String, Object> dbObject = dbObjectPk == null ? null :
            getObjectById(tableId, dbObjectPk);
        if (dbObject == null) {
            return saveObjectWithChildren(tableId, object, withChildrenDeep);
        }
        return updateObjectWithChildren(tableId, object, withChildrenDeep);
    }

    @Override
    public JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            JSONArray ja;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            ja = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).listObjectsByProperties(filter);
            return DictionaryMapUtils.mapJsonArray(ja, this.fetchDictionaryMapColumns(sourceInfoDao, tableInfo));
        } catch (Exception e) {
            throw new ObjectException(filter, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    private HashSet<String> collectPartFields(MetaTable tableInfo, String[] fields) {
        HashSet<String> fieldSet = new HashSet<>((fields.length + 5) * 3 / 2);
        for (TableField pkField : tableInfo.getPkFields()) {
            fieldSet.add(pkField.getPropertyName());
        }
        if (!"0".equals(tableInfo.getWorkFlowOptType())) {
            fieldSet.add(MetaTable.WORKFLOW_INST_ID_PROP);
            fieldSet.add(MetaTable.WORKFLOW_NODE_INST_ID_PROP);
        }
        if (BooleanBaseOpt.castObjectToBoolean(tableInfo.getUpdateCheckTimeStamp(), false)) {
            fieldSet.add(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
        }
        Collections.addAll(fieldSet, fields);
        return fieldSet;
    }

    private Map<String, Object> mapPoToDto(Map<String, Object> po) {
        Object obj = po.get(MetaTable.OBJECT_AS_CLOB_PROP);
        if (obj instanceof Map) {
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

    private JSONArray mapListPoToDto(JSONArray ja) {
        if (ja == null) {
            return null;
        }
        JSONArray jsonArray = new JSONArray(ja.size());
        for (Object json : ja) {
            if (json instanceof Map) {
                jsonArray.add(mapPoToDto((Map<String, Object>) json));
            } else {
                jsonArray.add(json);
            }
        }
        return jsonArray;
    }

    private Map<String, Object> mapDtoToPo(Map<String, Object> dto) {
        Map<String, Object> po = new HashMap<>(dto);
        po.remove(MetaTable.OBJECT_AS_CLOB_PROP);
        String jsonString = JSON.toJSONString(po);
        po.put(MetaTable.OBJECT_AS_CLOB_PROP, jsonString);
        return po;
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, String extFilter,
                                      Map<String, Object> params, String[] fields,
                                      PageDesc pageDesc) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        if (tableInfo == null) {
            throw new ObjectException("无此元数据表" + tableId);
        }
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            JSONArray objs;
            Object obj;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            HashSet<String> fieldSet = null;
            if (fields != null && fields.length > 0) {
                fieldSet = collectPartFields(tableInfo, fields);
            }
            Pair<String, TableField[]> q = (fieldSet == null) ?
                GeneralJsonObjectDao.buildFieldSqlWithFields(tableInfo, null, true)
                : GeneralJsonObjectDao.buildPartFieldSqlWithFields(tableInfo, fieldSet, null, false);

            String filter = GeneralJsonObjectDao.buildFilterSql(tableInfo, null, params);
            if (StringUtils.isNotBlank(extFilter)) {
                if (StringUtils.isNotBlank(filter)) {
                    filter = extFilter + " and " + filter;
                } else {
                    filter = extFilter;
                }
            }
            String sql = "select " + q.getLeft() + " from " + tableInfo.getTableName();
            if (StringUtils.isNotBlank(filter)) {
                sql = sql + " where " + filter;
            }
            String orderBy = GeneralJsonObjectDao.fetchSelfOrderSql(sql, params);
            if (StringUtils.isNotBlank(orderBy)) {
                sql = sql + " order by "
                    + QueryUtils.cleanSqlStatement(orderBy);
            }
            //小于0时查询不分页
            String querySql = pageDesc.getPageSize() < 0 ? sql : QueryUtils.buildLimitQuerySQL(sql,
                pageDesc.getRowStart(), pageDesc.getPageSize(), false,
                sourceInfo.getDBType());
            objs = GeneralJsonObjectDao.findObjectsByNamedSql(conn,
                querySql, params, q.getRight());
            String sGetCountSql = "select count(1) as totalRows from " + tableInfo.getTableName();
            if (StringUtils.isNotBlank(filter)) {
                sGetCountSql = sGetCountSql + " where " + filter;
            }
            obj = DatabaseAccess.getScalarObjectQuery(conn,
                sGetCountSql, params);
            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(obj));
            JSONArray ja = DictionaryMapUtils.mapJsonArray(objs, this.fetchDictionaryMapColumns(sourceInfoDao, tableInfo));
            if ("C".equals(tableInfo.getTableType())) {
                ja = mapListPoToDto(ja);
            }
            return ja;
        } catch (Exception e) {
            throw new ObjectException(params, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, Map<String, Object> params, String[] fields, PageDesc pageDesc) {
        return pageQueryObjects(tableId, null, params, fields, pageDesc);
    }

    @Override
    public JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc) {
        return pageQueryObjects(tableId, null, params, null, pageDesc);
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
            JSONArray objs;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            objs = dao.findObjectsByNamedSqlAsJSON(
                querySql, params, null, pageDesc.getPageNo(), pageDesc.getPageSize());
            pageDesc.setTotalRows(
                NumberBaseOpt.castObjectToInteger(DatabaseAccess.queryTotalRows(conn, querySql, params)));
            return DictionaryMapUtils.mapJsonArray(objs, this.fetchDictionaryMapColumns(sourceInfoDao, tableInfo));
        } catch (Exception e) {
            throw new ObjectException(params, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery(paramDriverSql, params);
        return pageQueryObjects(tableId, qap.getQuery(), qap.getParams(), pageDesc);
    }

    private List<DictionaryMapColumn> fetchDictionaryMapColumns(SourceInfoDao sourceInfoDao, MetaTable tableInfo) {
        if (tableInfo.getMdColumns() == null || tableInfo.getMdColumns().size() == 0) {
            return null;
        }
        List<DictionaryMapColumn> dictionaryMapColumns = new ArrayList<>(4);
        for (MetaColumn mc : tableInfo.getMdColumns()) {
            //dictionary; 解析 mc.getReferenceData() json
            if ("1".equals(mc.getReferenceType())) {
                setDictionaryColumns(dictionaryMapColumns, mc, false);
            } else if ("2".equals(mc.getReferenceType())) {
                String jsonStr = mc.getReferenceData().trim();
                String catalogCode = Md5Encoder.encodeBase64(jsonStr, true);
                boolean hasDictoinary = CodeRepositoryUtil.hasExtendedDictionary(catalogCode);
                if (!hasDictoinary) {
                    Object jsonObject = JSON.parse(jsonStr);
                    if (jsonObject instanceof Map) {
                        CodeRepositoryUtil.registeExtendedCodeRepo(catalogCode,
                            CollectionsOpt.objectMapToStringMap((Map) jsonObject));
                        hasDictoinary = true;
                    }
                }
                if (hasDictoinary) {
                    dictionaryMapColumns.add(new DictionaryMapColumn(
                        mc.getPropertyName(),
                        mc.getPropertyName() + "Desc",
                        catalogCode));
                }
            } else if ("3".equals(mc.getReferenceType())) {
                String sqlStr = mc.getReferenceData().trim();
                String catalogCode = Md5Encoder.encodeBase64(sqlStr, true);
                if (!CodeRepositoryUtil.hasExtendedDictionary(catalogCode)) {
                    CodeRepositoryUtil.registeExtendedCodeRepo(catalogCode,
                        new CachedObject<>(
                            new SqlDictionaryMapSupplier(
                                sourceInfoDao.getDatabaseInfoById(tableInfo.getDatabaseCode()),
                                sqlStr),
                            ICachedObject.KEEP_FRESH_PERIOD * 3));
                }
                DictionaryMapColumn dictionaryMapColumn = new DictionaryMapColumn(
                    mc.getPropertyName(),
                    mc.getPropertyName() + "Desc",
                    catalogCode);
                dictionaryMapColumn.setExpression(true);
                dictionaryMapColumns.add(dictionaryMapColumn);
            } else if ("4".equals(mc.getReferenceType())) {
                setDictionaryColumns(dictionaryMapColumns, mc, true);
            }
        }
        return dictionaryMapColumns;
    }

    private void setDictionaryColumns(List<DictionaryMapColumn> dictionaryMapColumns, MetaColumn mc, boolean isExpression) {
        if (mc.getReferenceData().startsWith("{")) {
            Object jsonObject = JSON.parse(mc.getReferenceData());
            if (jsonObject instanceof JSONObject) {
                for (Map.Entry<String, Object> ent : ((JSONObject) jsonObject).entrySet()) {
                    DictionaryMapColumn dictionaryMapColumn = new DictionaryMapColumn(
                        mc.getPropertyName(),
                        ent.getKey(),
                        StringBaseOpt.castObjectToString(ent.getValue())
                    );
                    dictionaryMapColumn.setExpression(isExpression);
                    dictionaryMapColumns.add(dictionaryMapColumn);
                }
            }
        } else {
            DictionaryMapColumn dictionaryMapColumn = new DictionaryMapColumn(
                mc.getPropertyName(),
                mc.getPropertyName() + "Desc",
                mc.getReferenceData()
            );
            dictionaryMapColumn.setExpression(isExpression);
            dictionaryMapColumns.add(dictionaryMapColumn);
        }
    }
}
