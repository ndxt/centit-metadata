package com.centit.support.dataopt.core;

import java.util.ArrayList;
import java.util.Map;

/**
 * 数据集 虚拟类
 */
public class SingleRowDataSet extends SimpleDataSet{

    public SingleRowDataSet(){
        super();
        data = new ArrayList<>(1);
    }

    public SingleRowDataSet( Map<String, Object> rowData){
        super();
        data = new ArrayList<>(1);
        data.add(rowData);
    }

    public void setRowData(Map<String, Object> rowData){
        if(data == null){
            data = new ArrayList<>(1);
            data.add(rowData);
        } else if(data.size() == 0){
            data.add(rowData);
        } else {
            data.set(0, rowData);
        }
    }

    public Map<String, Object> getRowData(){
        if(data == null || data.size() == 0){
            return null;
        } else {
            return data.get(0);
        }
    }
}
