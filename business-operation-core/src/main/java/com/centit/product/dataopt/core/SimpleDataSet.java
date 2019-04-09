package com.centit.product.dataopt.core;

import com.alibaba.fastjson.JSONArray;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 数据集 虚拟类
 */
public class SimpleDataSet implements DataSet, DataSetReader, Serializable {

    private static final long serialVersionUID = 5864945592001190336L;

    public SimpleDataSet(){
        dataSetName = DataSet.SINGLE_DATA_SET_DEFALUT_NAME;
        sorted = false;
    }

    public SimpleDataSet(String dataSetName){
        this.dataSetName = dataSetName;
        this.sorted = false;
    }

    public SimpleDataSet(List<Map<String, Object>> data) {
        this.data = data;
        this.dataSetName = DataSet.SINGLE_DATA_SET_DEFALUT_NAME;;
        this.sorted = false;
    }
    /**
     * 返回 DataSet 的名称
     */
    protected String dataSetName;
    /**
     * 返回 DataSet 的类型
     */
    protected String dataSetType;

    protected boolean sorted;

    /**
     * 返回 所有数据维度，这个维度是有序的，这个属性不是必须的
     */
    protected List<String> dimensions;
    /**
     * 数据集中的数据
     * 是一个 对象（Map）列表；可以类比为JSONArray
     */
    protected List<Map<String, Object>> data;
    /**
     * @return 是否已按照维度属性排序
     */

    @Override
    public String getDataSetName() {
        return dataSetName;
    }

    public void setDataSetName(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    @Override
    public String getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(String dataSetType) {
        this.dataSetType = dataSetType;
    }


    @Override
    public List<Map<String, Object>> getData() {
        return data;
    }


    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    public void setSorted(boolean sorted) {
        this.sorted = sorted;
    }

    @Override
    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public static SimpleDataSet fromJsonArray(JSONArray ja){
        SimpleDataSet dataSet = new SimpleDataSet();
        dataSet.setData((List)ja);
        return dataSet;
    }

    /**
     * 读取 dataSet 数据集
     * @param params 模块的自定义参数
     * @return dataSet 数据集
     */
    @Override
    public DataSet load(Map<String, Object> params) {
        return this;
    }
}
