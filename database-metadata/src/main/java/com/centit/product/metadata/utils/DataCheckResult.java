package com.centit.product.metadata.utils;

import com.centit.product.adapter.po.DataCheckRule;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.compiler.ObjectTranslate;
import com.centit.support.compiler.Pretreatment;
import com.centit.support.compiler.VariableFormula;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Data
public class DataCheckResult {
    /**
     * 验证结果
     */
    private  boolean result;
    /**
     * 验证错误消息提示
     */
    private List<String> errorMsgs;

    /**
     * @return 返回所有错误信息，null 没有错误。
     */
    public String getErrorMessage(){
        if(result){
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for(String msg : errorMsgs){
            sb.append(msg).append("\r\n");
        }
        return sb.toString();
    }

    public DataCheckResult(){
        result = true;
        errorMsgs = new ArrayList<>(4);
    }

    public static DataCheckResult create(){
        return new DataCheckResult();
    }

    /**
     * 规则校验
     * @param data 校验的对象
     * @param rule 规则
     * @param param 校验参数，key 包括： checkValue， param1， param2， param3,
     * @return 是否符合规则，和不符合错误提示
     */
    public DataCheckResult checkData(Object data, DataCheckRule rule, Map<String, String> param){
        Map<String, Object> realPparam = new HashMap<>();
        if(!param.isEmpty()){
            for(Map.Entry<String, String> ent : param.entrySet()){
                realPparam.put(ent.getKey(), VariableFormula.calculate(ent.getValue(), data));
            }
        }
        Map<String, Function<Object[], Object>> extraFunc = new HashMap<>();
        extraFunc.put("checkIdCardNo",
            (d) -> DataCheckUtils.checkIdCardNo(StringBaseOpt.castObjectToString(d[0])) );
        if(!BooleanBaseOpt.castObjectToBoolean(
                VariableFormula.calculate(rule.getRuleFormula(),new ObjectTranslate(realPparam) , extraFunc), false)){
            result = false;
            errorMsgs.add(Pretreatment.mapTemplateString(rule.getFaultMessage(), realPparam));
        }
        return this;
    }

}
