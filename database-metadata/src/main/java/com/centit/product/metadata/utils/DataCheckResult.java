package com.centit.product.metadata.utils;

import com.centit.product.metadata.po.DataCheckRule;
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
    private  Boolean result;
    /**
     * 验证错误消息提示
     */
    private List<String> errorMsgs;

    public static final  Map<String, Function<Object[], Object>> extraFunc = new HashMap<>();
    static {
        extraFunc.put("checkIdCardNo",
            (d) -> DataCheckUtils.checkIdCardNo(StringBaseOpt.castObjectToString(d[0])));
    }

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

    public void reset(){
        result = true;
        errorMsgs.clear();
    }

    public static DataCheckResult create(){
        return new DataCheckResult();
    }

    public DataCheckResult runCkeckRule(DataCheckRule rule, Map<String, Object> realParams,
                                     boolean makeErrorMessage, boolean nullAsTrue){
        Object checkValue = realParams.get(DataCheckRule.CHECK_VALUE_TAG);
        if(checkValue==null){
            if(!nullAsTrue) {
                result = false;
                if (makeErrorMessage) {
                    errorMsgs.add(Pretreatment.mapTemplateString(rule.getFaultMessage(), realParams));
                }
            }
        } else if(!BooleanBaseOpt.castObjectToBoolean(
            VariableFormula.calculate(rule.getRuleFormula(), new ObjectTranslate(realParams), extraFunc), false)){
            result = false;
            if(makeErrorMessage) {
                errorMsgs.add(Pretreatment.mapTemplateString(rule.getFaultMessage(), realParams));
            }
        }
        return this;
    }
    /**
     * 规则校验
     * @param data 校验的对象
     * @param rule 规则
     * @param param 校验参数，key 包括： checkValue， param1， param2， param3,
     * @param makeErrorMessage 不符合规范时返回的错误信息
     * @param nullAsTrue checkValue 是null的时候 忽略规则
     * @return 是否符合规则，和不符合错误提示
     */
    public DataCheckResult checkData(Object data, DataCheckRule rule, Map<String, String> param,
                                     boolean makeErrorMessage, boolean nullAsTrue){

        Map<String, Object> realPparam = new HashMap<>();
        if(!param.isEmpty()){
            for(Map.Entry<String, String> ent : param.entrySet()){
                realPparam.put(ent.getKey(), VariableFormula.calculate(ent.getValue(), data));
            }
        }

       return runCkeckRule(rule, realPparam, makeErrorMessage, nullAsTrue);
    }

    public DataCheckResult checkData(Object data, DataCheckRule rule, Map<String, String> param){
        return checkData( data, rule, param, true, true);
    }
}
