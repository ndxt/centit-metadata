package com.centit.product.dbdesign.pdmutils;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.product.adapter.po.MetaRelation;
import com.centit.product.adapter.po.PendingMetaColumn;
import com.centit.product.adapter.po.PendingMetaTable;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.UuidOpt;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.core.parameters.P;

import java.util.ArrayList;
import java.util.List;

public abstract class TableStoreJsonUtils {

    /*
    "tables": [
    {
      "tableInfo": {
        "tableId": "",
        "tableType": "",
        "tableName": "",
        "tableLabelName": "",
        "tableComment": ""
      },
      // 下面的 columns、indexes 参见 matedataJson.json
    "columns": [
        {
          "columnName": "字段名：代码",
          "fieldLabelName": "字段中文名称，标签名称",
          "columnType": "字段类型",
          "fieldType": "业务类型，字段类型相互转换",
          "maxLength": "最大长度",
          "scale": "精度",
          "precision" : "有效数据位数",
          "mandatory": "是否必填 true/false",
          "primaryKey": "是否为主键 true/false",
          "columnComment": "字段说明"
        }
      ],
      "indexes": [
        {
          "indexName": "索引名：代码",
          "indexType": "缩影类型 B+、FBI、BITMAP、IOT、Cluster",
          "indexComment": "索引说明",
          "indexFields": [
            "filedOne",
            "filedTwo"
          ]
        }
      ],
      "viewSql" : "这个仅仅是视图的时有内容"
    }
  ]
     */
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
        return null;
    }

    public static void freshenTablesId(List<PendingMetaTable> tables, List<MetaRelation> relations){

    }
}
