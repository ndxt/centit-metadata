package com.centit.product.metadata.utils;

import com.centit.product.adapter.po.DataCheckRule;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.common.LeftRightPair;
import com.centit.support.compiler.VariableFormula;

import java.util.HashMap;
import java.util.Map;

public abstract class DataCheckUtils {
    /**
     * 规则校验
     * @param data 校验的对象
     * @param rule 规则
     * @param param 校验参数，key 包括： checkValue， param1， param2， param3,
     * @return 是否符合规则，和不符合错误提示
     */
    public boolean checkData(Object data, DataCheckRule rule, Map<String, String> param){
        Map<String, Object> realPparam = new HashMap<>();
        if(!param.isEmpty()){
            for(Map.Entry<String, String> ent : param.entrySet()){
                realPparam.put(ent.getKey(), VariableFormula.calculate(ent.getValue(), data));
            }
        }
        //return new LeftRightPair<>(
        return BooleanBaseOpt.castObjectToBoolean(
                VariableFormula.calculate(rule.getRuleFormula(), realPparam), false);
          //  ), rule.getFaultMessage());
    }
}
