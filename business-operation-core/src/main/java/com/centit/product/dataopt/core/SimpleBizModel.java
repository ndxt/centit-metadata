package com.centit.product.dataopt.core;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.HashMap;
import java.util.Map;

public class SimpleBizModel implements BizModel{
    /**
     * 模型名称
     */
    private String modelName;
    /**
     * 模型的标识， 就是对应的主键
     * 或者对应关系数据库查询的参数（数据源参数）
     */
    private Map<String, Object> modelTag;
    /**
     * 模型数据
     */
    protected Map<String, DataSet> bizData;

    public SimpleBizModel(){

    }

    public SimpleBizModel(String modelName){
        this.modelName = modelName;
    }

    public void checkBizDataSpace(){
        if(this.bizData == null){
            this.bizData = new HashMap<>(6);
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public Map<String, Object> getModelTag() {
        return modelTag;
    }

    public void setModelTag(Map<String, Object> modelTag) {
        this.modelTag = modelTag;
    }

    public Map<String, DataSet> getBizData() {
        return bizData;
    }

    public void setBizData(Map<String, DataSet> bizData) {
        this.bizData = bizData;
    }
}
