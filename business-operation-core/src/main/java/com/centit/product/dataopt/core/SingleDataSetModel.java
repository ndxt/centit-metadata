package com.centit.product.dataopt.core;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.HashMap;

public class SingleDataSetModel extends SimpleBizModel {

    public SingleDataSetModel(){
        super();
        bizData = new HashMap<>(1);
    }

    public SingleDataSetModel(String modelName){
        super(modelName);
        bizData = new HashMap<>(1);
    }

    @Override
    public void checkBizDataSpace(){
        if(this.bizData == null){
            this.bizData = new HashMap<>(1);
        }
    }

    public SingleDataSetModel(DataSet dataSet){
        putMainDataSet(dataSet);
    }

    public SingleDataSetModel(String modelName, DataSet dataSet){
        setModelName(modelName);
        putMainDataSet(dataSet);
    }

    @JSONField(deserialize = false, serialize = false)
    public void setDataSet(DataSet dataSet){
        putMainDataSet(dataSet);
    }

    @JSONField(deserialize = false, serialize = false)
    public DataSet getDataSet(){
        return getMainDataSet();
    }
}

