package com.centit.support.dataopt.dataset;

import com.centit.support.dataopt.core.DataSet;

import java.util.Map;

public class CsvDataSet extends FileDataSet{

    /**
     * 读取 dataSet 数据集
     * @param params 模块的自定义参数
     * @return dataSet 数据集
     */
    @Override
    public DataSet load(Map<String, Object> params) {
        return null;
    }
    /**
     * 将 dataSet 数据集 持久化
     * @param dataSet 数据集
     */
    @Override
    public void save(DataSet dataSet) {

    }

    /**
     * 默认和 save 等效;
     * 对于文件类持久化方案来说可以差别化处理，比如添加到文件末尾
     * @param dataSet 数据集
     */
    @Override
    public void append(DataSet dataSet) {

    }

}
