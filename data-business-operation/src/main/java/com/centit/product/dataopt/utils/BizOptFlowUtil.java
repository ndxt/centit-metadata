package com.centit.product.dataopt.utils;

import com.alibaba.fastjson.JSONObject;
import com.centit.support.dataopt.bizopt.BuiltInOperation;
import com.centit.support.dataopt.bizopt.DataLoadSupplier;
import com.centit.support.dataopt.bizopt.PersistenceOperation;
import com.centit.support.dataopt.core.BizOperation;
import com.centit.support.dataopt.core.BizOptFlow;
import com.centit.support.dataopt.core.BizSupplier;
import org.apache.commons.lang3.StringUtils;

public abstract class BizOptFlowUtil {

    public static int runDataExchange(
        DataLoadSupplier loadData, PersistenceOperation saveData){
        BizOptFlow bof = new BizOptFlow().setSupplier(loadData).addOperation(saveData);
        return bof.run();
    }

    public static int runDataExchange(
        DataLoadSupplier loadData, BizOperation dataTrans, PersistenceOperation saveData){
        BizOptFlow bof = new BizOptFlow().setSupplier(loadData)
            .addOperation(dataTrans)
            .addOperation(saveData);
        return bof.run();
    }

    public static BizOptFlow createOptFlow(BizSupplier bizSupplier, String optDescJson){
        if(StringUtils.isBlank(optDescJson)){
            return new BizOptFlow().setSupplier(bizSupplier);
        }
        return createOptFlow(bizSupplier, JSONObject.parseObject(optDescJson));
    }

    public static BizOptFlow createOptFlow(BizSupplier bizSupplier, JSONObject optJson){
        return new BizOptFlow().setSupplier(bizSupplier)
            .addOperation(new BuiltInOperation(optJson));
    }
}
