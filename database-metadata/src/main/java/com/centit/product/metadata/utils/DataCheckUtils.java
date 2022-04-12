package com.centit.product.metadata.utils;

import org.apache.commons.lang3.StringUtils;

public abstract class DataCheckUtils {
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
        //32012378 1030153
        if (idCardNo.length() == 15) {
            for(int i=0;i<15;i++){
                if(idCard.charAt(i)<'0' || idCard.charAt(i)>'9'){
                    return false;
                }
            }
            int month = Integer.parseInt(idCard.substring(8,10));
            int day = Integer.parseInt(idCard.substring(10,12));
            return month>0 && month<13 && day>0 && day<32;
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
        int year = Integer.parseInt(idCard.substring(6,10));
        int month = Integer.parseInt(idCard.substring(10,12));
        int day = Integer.parseInt(idCard.substring(12,14));
        // 进行模11运算 codefan@sina.com
        mod = sum % 11;
        return VALIDATE[mod] == idCardNo.charAt(17) &&
                year > 1900 && year< 2100 && month>0 && month<13 && day>0 && day<32;
    }

}
