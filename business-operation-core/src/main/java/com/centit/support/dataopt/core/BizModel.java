package com.centit.support.dataopt.core;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Map;

public interface BizModel {
    SimpleBizModel EMPTY_BIZ_MODEL
        = new SimpleBizModel("EMPTY_BIZ_MODEL");
    String DEFAULT_MODEL_NAME = "bizModel";
    String DATABASE_CODE_TAG_NAME = "DATABASE_CODE_TAG_NAME";
    /**
     * @return 模型名称; 可以作为主DataSet的名称
     */
    String getModelName();
    /**
     * 模型的标识， 就是对应的主键
     * @return  或者对应关系数据库查询的参数（数据源参数）
     */
    Map<String, Object> getModelTag();
    /**
     * @return  模型数据
     */
    Map<String, DataSet> getBizData();


    default boolean isEmpty(){
        return getBizData() == null ||
            getBizData().isEmpty();
    }

    @JSONField(deserialize = false, serialize = false)
    default DataSet getMainDataSet(){
        if(!isEmpty()){
            return getBizData().get(getModelName());
        }
        return null;
    }

    void checkBizDataSpace();

    default void addDataSet(String dataSetName, DataSet dataSet) {
        checkBizDataSpace();
        getBizData().put(dataSetName, dataSet);
    }

    default DataSet fetchDataSetByName(String dataSetName){
        Map<String, DataSet> dss = getBizData();
        if(dss == null) {
            return null;
        }
        return dss.get(dataSetName);
    }
}
