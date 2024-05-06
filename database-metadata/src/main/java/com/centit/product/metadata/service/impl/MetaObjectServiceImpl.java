package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.DictionaryMapColumn;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.*;
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
import com.centit.support.database.orm.OrmUtils;
import com.centit.support.database.utils.*;
import com.centit.support.security.Md5Encoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Transactional
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
            String fieldName = col.getPropertyName();
            Object fieldValue = object.get(fieldName);

            if(fieldValue==null){
                fieldName = col.getColumnName();
                fieldValue = object.get(fieldName);
            }

            if (fieldValue != null) {
                switch (col.getFieldType()) {
                    case FieldType.DATE:
                        /*object.put(col.getPropertyName(), DatetimeOpt.castObjectToSqlDate(fieldValue));
                        break;*/
                    case FieldType.DATETIME:
                      /*  object.put(fieldName, DatetimeOpt.castObjectToDate(fieldValue));
                        break;*/
                    case FieldType.TIMESTAMP:
                        object.put(fieldName, DatetimeOpt.castObjectToSqlTimestamp(fieldValue));
                        break;
                    case FieldType.INTEGER:
                    case FieldType.LONG:
                        object.put(fieldName, NumberBaseOpt.castObjectToLong(fieldValue));
                        break;
                    case FieldType.MONEY:
                        object.put(fieldName, NumberBaseOpt.castObjectToBigDecimal(fieldValue));
                        break;
                    case FieldType.FLOAT:
                    case FieldType.DOUBLE:
                        object.put(fieldName, NumberBaseOpt.castObjectToDouble(fieldValue));
                        break;
                    case FieldType.STRING:
                    case FieldType.TEXT:
                        object.put(fieldName, StringBaseOpt.castObjectToString(fieldValue));
                        break;
                    case FieldType.BOOLEAN:
                        object.put(fieldName,
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
                                                   long pkOrder, boolean isGetObject,
                                                   boolean createNew)
        throws SQLException, IOException {

        for (MetaColumn field : metaTable.getMdColumns()) {
            if (StringUtils.equalsAny(field.getAutoCreateRule(), "C", "U", "S", "W", "F", "O")
               && ( "A".equals(field.getAutoCreateCondition()) ||
                    (createNew && object.get(field.getPropertyName()) == null))){
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
                    case "W": // Snowflake
                        object.put(field.getPropertyName(), OrmUtils.getDefaultSnowFlakeInstance().nextId());
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
                            throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                                "主键生成规则 SUB_ORDER 必须用于复合主键表中，并且只能用于整型字段！\n" +
                                    "SUB_ORDER must be used in composite primary key table, and can only be used for integer fields!");
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

    private SourceInfo fetchDatabaseInfo(String databaseCode) {
        return sourceInfoDao.getDatabaseInfoById(databaseCode);
    }

    private Map<String, Object> innerGetObjectById(final Connection conn, final MetaTable tableInfo, final Map<String, Object> pk)
        throws IOException, SQLException {
        GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
        if (pk.size() == 0) {
            throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                tableInfo.getTableName() + "没有传入主键,"+
                    "\n Missing primary key.");
        }
        Map<String, Object> dbObject;
        if (dao.checkHasAllPkColumns(pk)) {
            dbObject = dao.getObjectById(pk);
        } else if (pk.containsKey(MetaTable.WORKFLOW_INST_ID_PROP)) {
            dbObject = dao.getObjectByProperties(pk);
        } else {
            throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                 "表或者视图 " + tableInfo.getTableName()
                + " 缺少对应主键:" + JSON.toJSONString(pk) + ", Missing primary key.");
        }
        //添加数据字典转换
        return DictionaryMapUtils.mapJsonObject(dbObject, this.fetchDictionaryMapColumns(tableInfo));
    }

    private Map<String, Object> innerGetObjectPartFieldsById(final Connection conn, final MetaTable tableInfo,
                                                             final Map<String, Object> pk, String[] fields)
        throws IOException, SQLException {

        if (pk.size() == 0) {
            throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                tableInfo.getTableName() + "没有传入主键,\n Missing primary key.");
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
            throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                tableInfo.getTableName() + "没有传入主键,\n Missing primary key.");
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
    public MetaTable fetchTableInfo(String tableId){
        return metaDataCache.getTableInfo(tableId);
    }

    @Override
    public Map<String, Object> getObjectById(String tableId, Map<String, Object> pk) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            return innerGetObjectById(conn, tableInfo, pk);
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
                    this.fetchDictionaryMapColumns(parentTableInfo)));
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
                    .listObjectsByProperties(ref), this.fetchDictionaryMapColumns(subTableInfo));
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
                    ja = DictionaryMapUtils.mapJsonArray(ja, fetchDictionaryMapColumns(subTableInfo));
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
            mainObj = DictionaryMapUtils.mapJsonObject(mainObj, this.fetchDictionaryMapColumns(tableInfo));
            return fetchObjectParentAndChildren(tableInfo, mainObj, parents, children);
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
        } catch (SQLException | IOException e) {
            throw new ObjectException(mainObj, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
            mainObj = DictionaryMapUtils.mapJsonObject(mainObj, this.fetchDictionaryMapColumns(tableInfo));
            if (withChildrenDeep > 0 && mainObj != null) {
                fetchObjectRefrences(conn, mainObj, tableInfo, withChildrenDeep);
            }
            if(mainObj != null) {
                fetchObjectParents(conn, mainObj, tableInfo);
            }
            return mainObj;
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
            makeObjectValueByGenerator(objectMap, extParams, tableInfo, dao, 1l, true, true);

            fetchObjectParents(conn, objectMap, tableInfo);
            return objectMap;
        } catch (SQLException | IOException e) {
            throw new ObjectException(objectMap, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
        } catch (SQLException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
        } catch (SQLException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
        } catch (SQLException e) {
            throw new ObjectException(filterProperties, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
        } catch (SQLException e) {
            throw new ObjectException(fieldValues, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
        } catch (SQLException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
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
                if(withChildrenDeep>0 || dao.checkNeedUpdate(pobj.getLeft(), pobj.getRight())) {
                    resN += innerSaveObject(relTableInfo.getTableId(), pobj.getRight(), extParams, true, withChildrenDeep);
                }
            }
        }
        return resN;
    }

    private int innerSaveObject(String tableId, Map<String, Object> mainObj, Map<String, Object> extParams, boolean isUpdate, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        // 添加规则判段
        checkFieldRule(tableInfo, mainObj);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            if (isUpdate) {
                GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                makeObjectValueByGenerator(mainObj, extParams, tableInfo, dao, 1l, false, false);
                prepareObjectForSave(mainObj, tableInfo);
                dao.updateObject(mainObj);
            } else {
                GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
                makeObjectValueByGenerator(mainObj, extParams, tableInfo, dao, 1l, false, true);
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
                    Object subObjects = mainObj.get(md.getRelationName());
                    if(subObjects!=null) { // 必须不为 null， 如果需要删除 请赋值为[]
                        MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                        if ("T".equals(relTableInfo.getTableType())) {
                            List<Map<String, Object>> subTable = null;
                            Map<String, Object> ref = md.fetchChildFk(mainObj);

                            if (subObjects instanceof List) {
                                subTable = (List<Map<String, Object>>) subObjects;
                                for (Map<String, Object> subObj : subTable) {
                                    subObj.putAll(ref);
                                }
                            } else if (subObjects instanceof Map) {
                                subTable = new ArrayList<>(2);
                                ((Map<String, Object>) subObjects).putAll(ref);
                                subTable.add((Map<String, Object>) subObjects);
                            }

                            //List<MetaRelation> mdchilds = relTableInfo.getMdRelations();
                            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo);
                            this.replaceObjectsAsTabulation(dao, relTableInfo, subTable, extParams,
                                ref, withChildrenDeep - 1);
                        }
                    }
                }
            }
            return 1;
        } catch (SQLException |IOException e) {
            throw new ObjectException(mainObj, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
    public int updateObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep) {
        return innerSaveObject(tableId, object, extParams, true, withChildrenDeep);
    }

    /**
     * 更新数据，包括数据的子表信息，更新前检查版本信息是否一致，不一致抛出异常
     * @param tableId 表ID
     * @param object 数据记录
     * @param withChildrenDeep 子表层次
     * @return 大于0成功
     */
    @Override
    public int updateObjectWithChildrenCheckVersion(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep){
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        List<String> fields = tableInfo.extraVersionFields();
        if(fields == null || fields.size()==0){
            throw new ObjectException(object, ObjectException.SYSTEM_CONFIG_ERROR,
                "元数据配置不完整，缺少更新版本标识信息：" + tableId+
                "\n Metadata configuration incomplete, missing update version identifier information: " + tableId);
        }

        try {
            SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            Map<String, Object> mainObj = dao.getObjectById(object);
            for(String field : fields){
                if(! GeneralAlgorithm.equals(mainObj.get(field), object.get(field))){
                    throw new ObjectException(object, ObjectException.DATA_VALIDATE_ERROR,
                        "跟新前版本校验失败，数据可能已经被其他业务更改：" + tableId +
                        "\n Data version check failed before updating, data may have been changed by other business: " + tableId);
                }
            }
        } catch (SQLException | IOException e) {
            throw new ObjectException(object, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }

        return innerSaveObject(tableId, object, null, true, withChildrenDeep);
    }

    @Override
    public void deleteObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
            Map<String, Object> mainObj = dao.getObjectById(pk);
            if (null == mainObj || mainObj.size() == 0) {
                return;
            }
            if(withChildrenDeep > 0) {
                List<MetaRelation> mds = tableInfo.getMdRelations();
                if (mds != null) {
                    for (MetaRelation md : mds) {
                        MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                        if ("T".equals(relTableInfo.getTableType())) {
                            List<MetaRelation> mdChilds = relTableInfo.getMdRelations();
                            if (mdChilds != null && withChildrenDeep > 1) {
                                JSONArray children = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                    .listObjectsByProperties(md.fetchChildFk(mainObj));
                                if (children != null && children.size() > 0) {
                                    for (Object obj : children) {
                                        deleteObjectWithChildren(relTableInfo.getTableId(),
                                            (Map<String, Object>) obj, withChildrenDeep - 1);
                                    }
                                }
                            } else {
                                GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                    .deleteObjectsByProperties(md.fetchChildFk(mainObj));
                            }
                        }
                    }
                }
            }
            dao.deleteObjectById(pk);
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    private void innerSoftDeleteObject(Connection conn, MetaTable tableInfo, Map<String, Object> pk, int withChildrenDeep)
        throws SQLException, IOException {
        GeneralJsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);
        Map<String, Object> mainObj = dao.getObjectById(pk);
        if(StringUtils.isNotBlank(tableInfo.getDeleteTagField())){
            Map<String, Object> deleteTag = tableInfo.extraDeleteTag();
            mainObj.putAll(deleteTag);
            dao.updateObject(deleteTag.keySet(), mainObj);
        }
        if(withChildrenDeep > 0) {
            List<MetaRelation> mds = tableInfo.getMdRelations();
            if (mds != null) {
                for (MetaRelation md : mds) {
                    MetaTable relTableInfo = metaDataCache.getTableInfo(md.getChildTableId());
                    if ("T".equals(relTableInfo.getTableType())) {
                        List<MetaRelation> mdChilds = relTableInfo.getMdRelations();
                        if (mdChilds != null && withChildrenDeep > 1) {
                            JSONArray children = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo)
                                .listObjectsByProperties(md.fetchChildFk(mainObj));
                            if (children != null && children.size() > 0) {
                                for (Object obj : children) {
                                    innerSoftDeleteObject(conn, relTableInfo,
                                        (Map<String, Object>) obj, withChildrenDeep - 1);
                                }
                            }
                        } else {
                            if(StringUtils.isNotBlank(relTableInfo.getDeleteTagField())) {
                                Map<String, Object> deleteTag = relTableInfo.extraDeleteTag();
                                GeneralJsonObjectDao subTableDao = GeneralJsonObjectDao.createJsonObjectDao(conn, relTableInfo);
                                subTableDao.updateObjectsByProperties(deleteTag.keySet(), deleteTag, md.fetchChildFk(mainObj));
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 逻辑删除数据，包括数据的子表信息， 前提是元数据中保存了逻辑删除信息，如果没有将会抛出异常
     * @param tableId 表ID
     * @param pk 主键数据
     * @param withChildrenDeep 子表层次
     */
    @Override
    public void softDeleteObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep){
        MetaTable tableInfo = metaDataCache.getTableInfoWithRelations(tableId);
        if(StringUtils.isBlank(tableInfo.getDeleteTagField())){
            throw new ObjectException(pk, ObjectException.SYSTEM_CONFIG_ERROR,
                "元数据配置不完整，缺少逻辑删除标记信息：" + tableId+
                "\n The metadata configuration is incomplete, missing logical deletion mark information: " + tableId);
        }
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            innerSoftDeleteObject(conn, tableInfo, pk, withChildrenDeep);
        } catch (SQLException | IOException e) {
            throw new ObjectException(pk, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public int mergeObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams, int withChildrenDeep) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        Map<String, Object> dbObjectPk = tableInfo.fetchObjectPk(object);
        Map<String, Object> dbObject = dbObjectPk == null ? null :
            getObjectById(tableId, dbObjectPk);
        if (dbObject == null) {
            return saveObjectWithChildren(tableId, object, extParams, withChildrenDeep);
        }
        return updateObjectWithChildren(tableId, object, extParams, withChildrenDeep);
    }

    @Override
    public JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        SourceInfo sourceInfo = fetchDatabaseInfo(tableInfo.getDatabaseCode());
        try {
            JSONArray ja;
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            ja = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo).listObjectsByProperties(filter);
            return DictionaryMapUtils.mapJsonArray(ja, this.fetchDictionaryMapColumns(tableInfo));
        } catch (SQLException | IOException e) {
            throw new ObjectException(filter, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
            throw new ObjectException(ObjectException.DATA_NOT_FOUND_EXCEPTION,
                "无此元数据表" + tableId+
                "\n There is no metadata table: " + tableId);
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
            if ("C".equals(tableInfo.getTableType())) {
                objs = mapListPoToDto(objs);
            }
            return DictionaryMapUtils.mapJsonArray(objs, this.fetchDictionaryMapColumns(tableInfo));
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
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
            return DictionaryMapUtils.mapJsonArray(objs, this.fetchDictionaryMapColumns(tableInfo));
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    @Override
    public JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery(paramDriverSql, params);
        return pageQueryObjects(tableId, qap.getQuery(), qap.getParams(), pageDesc);
    }

    private List<DictionaryMapColumn> fetchDictionaryMapColumns(MetaTable tableInfo) {
        if (tableInfo.getMdColumns() == null || tableInfo.getMdColumns().size() == 0) {
            return null;
        }
        List<DictionaryMapColumn> dictionaryMapColumns = new ArrayList<>(4);
        for (MetaColumn mc : tableInfo.getMdColumns()) {
            //dictionary; 解析 mc.getReferenceData() json
            //引用类型 0：没有：1： 数据字典 2：JSON表达式 3：sql语句  4：复合数据字典
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
                // catalogCode 不能仅仅以sql语句为标准，还需要添加上对应的数据库
                String catalogCode = Md5Encoder.encodeBase64(tableInfo.getDatabaseCode() + sqlStr, true);
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

    @Override
    public Map<String, String> fetchColumnRefData(String tableId, String columnCode, String topUnit, String lang) {
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        MetaColumn mc = tableInfo.findFieldByColumn(columnCode);
        //引用类型 0：没有：1： 数据字典 2：JSON表达式 3：sql语句  4：复合数据字典
        if ("1".equals(mc.getReferenceType())) {
            return CodeRepositoryUtil.getLabelValueMap(mc.getReferenceData(), topUnit, lang);
        } else if ("2".equals(mc.getReferenceType())) {
            String jsonStr = mc.getReferenceData().trim();
            Object jsonObject = JSON.parse(jsonStr);
            if(jsonObject instanceof JSONObject) {
                return CollectionsOpt.objectMapToStringMap((Map)jsonObject);
            }
            return null;
        } else if ("3".equals(mc.getReferenceType())) {
            String sqlStr = mc.getReferenceData().trim();
            SqlDictionaryMapSupplier mapSupplier= new SqlDictionaryMapSupplier(
                sourceInfoDao.getDatabaseInfoById(tableInfo.getDatabaseCode()),
                sqlStr);
            return mapSupplier.get();
        } else if ("4".equals(mc.getReferenceType())) {
            Object jsonObject = JSON.parse(mc.getReferenceData());
            Map<String, String> datamap = new HashMap<>(100);
            if (jsonObject instanceof JSONObject) {
                for (Map.Entry<String, Object> ent : ((JSONObject) jsonObject).entrySet()) {
                    Map<String, String> dictMap = CodeRepositoryUtil.getLabelValueMap(
                        StringBaseOpt.castObjectToString(ent.getValue()), topUnit, lang);

                    if(dictMap!=null)
                        datamap.putAll(dictMap);
                }
            }
            return datamap;
        }
        return null;
    }

    private static void setDictionaryColumns(List<DictionaryMapColumn> dictionaryMapColumns, MetaColumn mc, boolean isExpression) {
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

    /**
     * 根据sql查询条件语句返回查询结果
     * @param databaseCode 数据库代码
     * @param namedSql 条件语句，仅仅是条件部分
     * @param params 过滤条件
     * @return 查询结果
     */
    @Override
    public JSONArray queryDatas(String databaseCode, String namedSql, Map<String, Object> params){
        SourceInfo sourceInfo = fetchDatabaseInfo(databaseCode);
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, namedSql, params);
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }

    /**
     * 根据sql查询条件语句返回查询结果
     * @param databaseCode 数据库代码
     * @param namedSql 条件语句，仅仅是条件部分
     * @param params 过滤条件
     * @param pageDesc 分页信息
     * @return 查询结果
     */
    @Override
    public JSONArray pageQueryDatas(String databaseCode, String namedSql, Map<String, Object> params, PageDesc pageDesc){
        SourceInfo sourceInfo = fetchDatabaseInfo(databaseCode);
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
            String sGetCountSql =  QueryUtils.buildGetCountSQL(namedSql);
            Object obj = DatabaseAccess.getScalarObjectQuery(conn,
                sGetCountSql, params);
            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(obj));

            return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, namedSql, params, null,
                pageDesc.getPageNo(), pageDesc.getPageSize());
        } catch (SQLException | IOException e) {
            throw new ObjectException(params, ObjectException.DATABASE_OPERATE_EXCEPTION, e);
        }
    }
}
