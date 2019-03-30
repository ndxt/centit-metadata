package com.centit.product.dataopt.core;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleObjectModel extends SimpleBizModel {

    public SingleObjectModel(){
        super();
        bizData = new HashMap<>(1);
    }

    public SingleObjectModel(Object obj){
        super();
        bizData = new HashMap<>(1);
        putMainDataSet(new SingleObjectDataSet(obj));
    }

    @Override
    public void checkBizDataSpace(){
        if(this.bizData == null){
            this.bizData = new HashMap<>(1);
        }
    }

    @JSONField(deserialize = false, serialize = false)
    public void setObject(Object obj){
        checkBizDataSpace();
        putMainDataSet(new SingleObjectDataSet(obj));
    }

    @JSONField(deserialize = false, serialize = false)
    public Object getObject(){
        DataSet dataSet = getMainDataSet();
        if(dataSet==null) {
            return null;
        }
        List<Map<String, Object>> data = dataSet.getData();
        if (data == null || data.size() == 0) {
            return null;
        } else {
            return data.get(0).get(SingleObjectDataSet.SINGLE_DATA_FIELD_NAME);
        }
    }
}

