package com.centit.product.datapacket.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.product.dataopt.bizopt.BuiltInOperation;
import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.utils.DataSetOptUtil;
import com.centit.product.datapacket.vo.ColumnSchema;
import com.centit.product.datapacket.vo.DataPacketSchema;
import com.centit.product.datapacket.vo.DataSetSchema;
import com.centit.support.algorithm.StringBaseOpt;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public abstract class DataPacketUtil {
    public static DataPacketSchema calcDataPacketSchema(DataPacketSchema sourceSchema, JSONObject bizOptJson){
        JSONArray optSteps = bizOptJson.getJSONArray("steps");
        if(optSteps==null || optSteps.isEmpty()){
            return sourceSchema;
        }
        DataPacketSchema result = sourceSchema;
        for(Object step : optSteps){
            if(step instanceof JSONObject){
                calcSchemaOneStep(result, (JSONObject)step);
            }
        }
        return result;
    }

    public static DataPacketSchema  calcSchemaOneStep(DataPacketSchema sourceSchema, JSONObject bizOptJson){
        String sOptType = bizOptJson.getString("operation");
        if(StringUtils.isBlank(sOptType)) {
            return sourceSchema;
        }
        switch (sOptType){
            case "map":
                return calcSchemaMap(sourceSchema, bizOptJson);
            case "filter":
                return calcSchemaFilter(sourceSchema, bizOptJson);
            case "append":
                return calcSchemaAppend(sourceSchema, bizOptJson);
            case "stat":
                return calcSchemaStat(sourceSchema, bizOptJson);
            case "analyse":
                return calcSchemaAnalyse(sourceSchema, bizOptJson);
            case "cross":
                return calcSchemaCross(sourceSchema, bizOptJson);
            case "compare":
                return calcSchemaCompare(sourceSchema, bizOptJson);
            case "join":
                return calcSchemaJoin(sourceSchema, bizOptJson);
            case "union":
                return calcSchemaUnion(sourceSchema, bizOptJson);
             default:
                return sourceSchema;
        }
    }

    public static DataPacketSchema calcSchemaMap(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sourDSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", sourceSchema.getPacketName());
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourDSName);
        Object mapInfo = bizOptJson.get("fieldsMap");
        if(mapInfo instanceof Map){
            DataSetSchema dss = new DataSetSchema(targetDSName);
            dss.setDataSetTitle(sourDSName+":map");
            for(Object s : ((Map)mapInfo).keySet()){
                dss.addColumnIfNotExist(StringBaseOpt.castObjectToString(s));
            }
            sourceSchema.putDataSetSchema(dss);
        }
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaAppend(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sourDSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", sourceSchema.getPacketName());
        //String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourDSName);
        Object mapInfo = bizOptJson.get("fieldsMap");
        DataSetSchema dss = sourceSchema.fetchDataSetSchema(sourDSName);
        if(dss != null && mapInfo instanceof Map){
            for(Object s : ((Map)mapInfo).keySet()){
                dss.addColumnIfNotExist(StringBaseOpt.castObjectToString(s));
            }
        }
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaFilter(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sourDSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", sourceSchema.getPacketName());
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourDSName);
        DataSetSchema dss = sourceSchema.fetchDataSetSchema(sourDSName);
        dss.setDataSetTitle(sourDSName+":filter");
        dss.setDataSetId(targetDSName);
        dss.setDataSetName(targetDSName);
        sourceSchema.putDataSetSchema(dss);
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaStat(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sourDSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", sourceSchema.getPacketName());
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourDSName);
        Object groupBy = bizOptJson.get("groupBy");
        List<String> groupFields = StringBaseOpt.objectToStringList(groupBy);
        DataSetSchema sdss = sourceSchema.fetchDataSetSchema(sourDSName);
        DataSetSchema dss = new DataSetSchema(targetDSName);
        if(groupFields!=null){
            for(String f : groupFields){
                dss.addColumns(sdss.fetchColumn(f));
            }
        }
        Object stat = bizOptJson.get("fieldsMap");
        if(stat instanceof Map){


            sourceSchema.putDataSetSchema(dss);
        }
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaAnalyse(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sourDSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", sourceSchema.getPacketName());
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourDSName);

        Object orderBy = bizOptJson.get("orderBy");
        List<String> orderFields = StringBaseOpt.objectToStringList(orderBy);
        Object groupBy = bizOptJson.get("groupBy");
        List<String> groupFields = StringBaseOpt.objectToStringList(groupBy);

        Object analyse = bizOptJson.get("fieldsMap");
        if(analyse instanceof Map){
            DataSetSchema dss = new DataSetSchema(targetDSName);

            sourceSchema.putDataSetSchema(dss);
        }
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaCross(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sourDSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", sourceSchema.getPacketName());
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourDSName);
        Object rowHeader = bizOptJson.get("rowHeader");
        List<String> rows = StringBaseOpt.objectToStringList(rowHeader);
        Object colHeader = bizOptJson.get("colHeader");
        List<String> cols = StringBaseOpt.objectToStringList(colHeader);

        DataSetSchema dss = new DataSetSchema(targetDSName);

        sourceSchema.putDataSetSchema(dss);

        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaCompare(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sour1DSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", null);
        String sour2DSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source2", null);
        if(sour1DSName == null || sour2DSName ==null ){
            return sourceSchema;
        }

        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourceSchema.getPacketName());
        Object primaryKey = bizOptJson.get("primaryKey");
        List<String> pks = StringBaseOpt.objectToStringList(primaryKey);
        DataSetSchema dss = new DataSetSchema(targetDSName);

        sourceSchema.putDataSetSchema(dss);
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaJoin(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sour1DSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", null);
        String sour2DSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source2", null);
        List<String> pks = StringBaseOpt.objectToStringList(bizOptJson.get("primaryKey"));
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourceSchema.getPacketName());
        DataSetSchema dss = new DataSetSchema(targetDSName);

        sourceSchema.putDataSetSchema(dss);
        return sourceSchema;
    }

    public static DataPacketSchema calcSchemaUnion(DataPacketSchema sourceSchema, JSONObject bizOptJson) {
        String sour1DSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source", null);
        String sour2DSName = BuiltInOperation.getJsonFieldString(bizOptJson,"source2", null);
        String targetDSName = BuiltInOperation.getJsonFieldString(bizOptJson, "target", sourceSchema.getPacketName());
        DataSetSchema dss = new DataSetSchema(targetDSName);

        sourceSchema.putDataSetSchema(dss);
        return sourceSchema;
    }
}
