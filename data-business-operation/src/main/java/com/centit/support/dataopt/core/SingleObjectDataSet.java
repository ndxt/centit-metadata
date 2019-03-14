package com.centit.support.dataopt.core;

import com.centit.support.algorithm.CollectionsOpt;

import java.util.ArrayList;

/**
 * 数据集 虚拟类
 */
public class SingleObjectDataSet  extends SimpleDataSet {
    public static final String SINGLE_DATA_FIELD_NAME = "data";
    public SingleObjectDataSet() {
        super();
        data = new ArrayList<>(1);
    }

    public SingleObjectDataSet(Object obj) {
        super();
        data = new ArrayList<>(1);
        data.add(CollectionsOpt.createHashMap(SINGLE_DATA_FIELD_NAME, obj));
    }

    public void setObject(Object obj) {
        if(data == null){
            data = new ArrayList<>(1);
            data.add(CollectionsOpt.createHashMap(SINGLE_DATA_FIELD_NAME, obj));
        } else if ( data.size() == 0) {
            data.add(CollectionsOpt.createHashMap(SINGLE_DATA_FIELD_NAME, obj));
        } else {
            data.set(0, CollectionsOpt.createHashMap(SINGLE_DATA_FIELD_NAME, obj));
        }
    }

    public Object getObject() {
        if (data == null || data.size() == 0) {
            return null;
        } else {
            return data.get(0).get(SINGLE_DATA_FIELD_NAME);
        }
    }
}
