package com.centit.product.metadata.utils;

import com.centit.product.adapter.po.DataCheckRule;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.LeftRightPair;
import com.centit.support.compiler.ObjectTranslate;
import com.centit.support.compiler.Pretreatment;
import com.centit.support.compiler.VariableFormula;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class DataCheckUtils {
    /**
     * 规则校验
     * @param data 校验的对象
     * @param rule 规则
     * @param param 校验参数，key 包括： checkValue， param1， param2， param3,
     * @return 是否符合规则，和不符合错误提示
     */
  /*  @Deprecated
    public static boolean checkData(Object data, DataCheckRule rule, Map<String, String> param){
        Map<String, Object> realPparam = new HashMap<>();
        if(!param.isEmpty()){
            for(Map.Entry<String, String> ent : param.entrySet()){
                realPparam.put(ent.getKey(), VariableFormula.calculate(ent.getValue(), data));
            }
        }
        Map<String, Function<Object[], Object>> extraFunc = new HashMap<>();
        extraFunc.put("checkIdCardNo",
            (d) -> DataCheckUtils.checkIdCardNo(StringBaseOpt.castObjectToString(d[0])) );
        return BooleanBaseOpt.castObjectToBoolean(
                VariableFormula.calculate(rule.getRuleFormula(),new ObjectTranslate(realPparam) , extraFunc), false);
    }*/


    /**
     * 十七位数字本体码权重
     */
    private static final int[] WEIGHT = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};
    /**
     * mod11,对应校验码字符值
     */
    private static final char[] VALIDATE = {'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};

    static boolean checkIdCardNo(String idCard) {
        if (StringUtils.isBlank(idCard)) {
            return false;
        }
        String idCardNo = idCard.trim();
        if (idCardNo.length() == 15) {
            return true;
        }
        if (idCardNo.length() != 18) {
            return false;
        }
        // wi*Ai和
        int sum = 0;
        // 进行模11运算
        int mod;

        for (int i = 0; i < 17; i++) {
            char curChar = idCardNo.charAt(i);
            if (curChar < '0' || curChar > '9') {
                return false;
            }
            sum += (idCardNo.charAt(i) - '0') * WEIGHT[i];
        }
        // 进行模11运算
        mod = sum % 11;
        return VALIDATE[mod] == idCardNo.charAt(17);
    }

}
