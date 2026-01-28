package com.centit.product.metadata.utils;

import com.centit.product.metadata.po.DataCheckRule;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.compiler.ObjectTranslate;
import com.centit.support.compiler.Pretreatment;
import com.centit.support.compiler.VariableFormula;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Data
public class DataCheckResult {
    /**
     * 验证结果 true 表示通过 false 表示未通过
     */
    private  Boolean result;
    /**
     * 验证错误消息提示
     */
    private StringBuilder errorMsg;

    public static final  Map<String, Function<Object[], Object>> extraFunc = new HashMap<>();
    static {
        extraFunc.put("checkIdCardNo",
            (d) -> DataCheckUtils.checkIdCardNo(StringBaseOpt.castObjectToString(d[0])));
        extraFunc.put("inDictionary",
            DataCheckUtils::inDictionary);
    }

    /**
     * @return 返回所有错误信息，null 没有错误。
     */
    public String getErrorMessage(){
        if(result){
            return null;
        }
        return errorMsg.toString();
    }

    public DataCheckResult(){
        result = true;
        errorMsg = new StringBuilder();
    }

    public void reset(){
        result = true;
        errorMsg.setLength(0);
    }

    public static DataCheckResult create(){
        return new DataCheckResult();
    }

    private void runCheckRule(String fieldName, DataCheckRule rule, Map<String, Object> realParams,
                                        boolean makeErrorMessage, boolean nullAsTrue){
        Object checkValue = realParams.get(DataCheckRule.CHECK_VALUE_TAG);
        if(checkValue == null && nullAsTrue){
            return;
        }
        if(!BooleanBaseOpt.castObjectToBoolean(
            VariableFormula.calculate(rule.getRuleFormula(), new ObjectTranslate(realParams), extraFunc), false)){
            result = false;
            if(makeErrorMessage) {
                if(errorMsg.length()>0){
                    errorMsg.append("\r\n");
                }
                errorMsg.append(fieldName).append(":")
                    .append(Pretreatment.mapTemplateString(rule.getFaultMessage(), realParams));
            }
        }
    }
    /**
     * 规则校验
     * @param data 校验的对象
     * @param rule 规则
     * @param param 校验参数，key 包括： checkValue， param1， param2， param3,
     * @param makeErrorMessage 不符合规范时返回的错误信息
     * @param nullAsTrue checkValue 是null的时候 忽略规则
     */
    public void checkData(Object data, DataCheckRule rule, Map<String, String> param,
                                     boolean makeErrorMessage, boolean nullAsTrue){
        String checkFieldName = param.get(DataCheckRule.CHECK_VALUE_TAG);
        Map<String, Object> realParams = new HashMap<>();
        if(!param.isEmpty()){
            for(Map.Entry<String, String> ent : param.entrySet()){
                String fieldName = ent.getValue();
                if(fieldName.startsWith("f:")){
                    realParams.put(ent.getKey(), VariableFormula.calculate(fieldName.substring(2), data));
                } else if(DataCheckRule.CHECK_VALUE_TAG.equals(ent.getKey())){
                    realParams.put(ent.getKey(), VariableFormula.calculate(fieldName, data));
                } else {
                    Object value = VariableFormula.calculate(fieldName, data);
                    if (value == null) {
                        value = fieldName;
                    }
                    realParams.put(ent.getKey(), value);
                }
                realParams.put(ent.getKey(), VariableFormula.calculate(ent.getValue(), data));
            }
        }
        runCheckRule(checkFieldName, rule, realParams, makeErrorMessage, nullAsTrue);
    }

}
