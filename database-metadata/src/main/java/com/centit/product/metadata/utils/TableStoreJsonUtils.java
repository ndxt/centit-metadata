package com.centit.product.metadata.utils;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.product.metadata.po.MetaRelDetail;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.PendingMetaColumn;
import com.centit.product.metadata.po.PendingMetaTable;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;

import java.util.ArrayList;
import java.util.List;

public abstract class TableStoreJsonUtils {

    public static List<PendingMetaTable> fetchTables(JSONObject json){
        List<PendingMetaTable> tableList = new ArrayList<>();
        JSONArray tables = json.getJSONArray("tables");
        if(tables!=null && tables.size()>0) {
            for(Object obj : tables){
                if(obj instanceof JSONObject) {
                    JSONObject tableJson = (JSONObject) obj;
                    JSONObject tableInfo = tableJson.getJSONObject("tableInfo");
                    PendingMetaTable table1 = new PendingMetaTable();
                    if(tableInfo!=null) {
                        table1.setTableId(tableInfo.getString("tableId"));
                        table1.setTableType(tableInfo.getString("tableType"));
                        table1.setTableName(tableInfo.getString("tableName"));
                        table1.setTableLabelName(tableInfo.getString("tableLabelName"));
                        table1.setTableComment(tableInfo.getString("tableComment"));
                    }
                    table1.setViewSql(tableJson.getString("viewSql") );
                    JSONArray columnsJson = tableJson.getJSONArray("columns");
                    long nColNo = 0;
                    if(columnsJson!=null && columnsJson.size()>0){
                        List<PendingMetaColumn> mdColumns = new ArrayList<>();
                        for(Object col : columnsJson){
                            if(col instanceof JSONObject) {
                                JSONObject colJson = (JSONObject) col;
                                PendingMetaColumn column = new PendingMetaColumn();
                                column.setColumnName(colJson.getString("columnName"));
                                column.setFieldLabelName(colJson.getString("fieldLabelName"));
                                //column.setColumnType(colJson.getString("columnType"));
                                column.setFieldType(colJson.getString("fieldType"));
                                column.setMaxLength(NumberBaseOpt.castObjectToInteger(colJson.get("maxLength")));
                                column.setScale(NumberBaseOpt.castObjectToInteger(colJson.get("scale")));
                                //column.setPrecision(colJson.getString("precision"));
                                column.setMandatory(BooleanBaseOpt.castObjectToBoolean(colJson.get("mandatory"),false));
                                column.setPrimaryKey(BooleanBaseOpt.castObjectToBoolean(colJson.get("primaryKey"),false));
                                column.setColumnComment(colJson.getString("columnComment"));
                                column.setColumnOrder(++nColNo);
                                mdColumns.add(column);
                            }
                        }
                        table1.setMdColumns(mdColumns);
                    }
                    tableList.add(table1);
                }
            }
        }
        return tableList;
    }

    public static List<MetaRelation> fetchRelations(JSONObject json){
        List<MetaRelation> relationList = new ArrayList<>();
        JSONArray modules = json.getJSONArray("modules");
        if(modules!=null && modules.size()>0) { // 外层模块循环
            for (Object obj : modules) {
                if (obj instanceof JSONObject) {
                    JSONObject moduleJson = (JSONObject) obj;
                    JSONArray relations = moduleJson.getJSONArray("relations");
                    if(relations != null && relations.size()>0){
                        for (Object relObj : relations) { // 内层模块的关联信息循环
                            if (relObj instanceof JSONObject) {
                                JSONObject relationJson = (JSONObject) relObj;
                                //获取 关联信息
                                JSONObject relationInfo = relationJson.getJSONObject("info");
                                if(relationInfo!=null){
                                    MetaRelation relation = new MetaRelation();
                                    relation.setRelationName(relationInfo.getString("relationsName"));
                                    relation.setParentTableId(relationInfo.getString("parentTable"));
                                    relation.setChildTableId(relationInfo.getString("chileTable"));
                                    relation.setRelationComment(relationInfo.getString("relationsDesc"));

                                    JSONArray refCols = json.getJSONArray("referenceColumns");
                                    if (refCols != null && refCols.size() > 0) {
                                        List<MetaRelDetail> relationDetails = new ArrayList<>();
                                        for (Object colObj : refCols) { // 关联字段循环
                                            if (colObj instanceof JSONObject) {
                                                JSONObject columnJson = (JSONObject) colObj;
                                                MetaRelDetail detail = new MetaRelDetail();
                                                detail.setParentColumnCode(columnJson.getString("parentColumn"));
                                                detail.setChildColumnCode(columnJson.getString("childColumn"));
                                                relationDetails.add(detail);
                                            }
                                        }
                                        relation.setRelationDetails(relationDetails);
                                    }
                                    relationList.add(relation);
                                }
                            }
                        }
                    }
                }
            }
        }
        return relationList;
    }

}
