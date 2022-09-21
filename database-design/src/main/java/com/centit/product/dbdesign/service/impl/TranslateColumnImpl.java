package com.centit.product.dbdesign.service.impl;


import com.alibaba.fastjson.JSON;
import com.aliyun.alimt20181012.models.TranslateGeneralResponse;
import com.aliyun.tea.TeaException;
import com.centit.product.dbdesign.service.TranslateColumn;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.FieldType;
import com.centit.support.security.AESSecurityUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import java.util.Locale;

public class TranslateColumnImpl implements TranslateColumn {

    @Value("${translate.service.aliyun.access.key:}")
    private String accessKeyId;
    @Value("${translate.service.aliyun.access.secret:}")
    private String accessKeySecret;

    /**
     * 使用AK SK初始化账号Client
     * @return Client
     * @throws Exception 异常
     */
    public com.aliyun.alimt20181012.Client createClient() throws Exception {
        if(StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(accessKeySecret)){
            accessKeyId = "cipher:+GrP3D07U/aR2WDtm9iTSUeJ0F00X0f75Byebbcw8fc=";
            accessKeySecret = "cipher:gqdjhi7JEasb2uiOW/riueAXA4vvOxsgYfmdRbAqwIU=";
        }
        com.aliyun.teaopenapi.models.Config config = new com.aliyun.teaopenapi.models.Config()
            // 您的 AccessKey ID
            .setAccessKeyId(AESSecurityUtils.decryptParameterString(accessKeyId))
            // 您的 AccessKey Secret
            .setAccessKeySecret(AESSecurityUtils.decryptParameterString(accessKeySecret));
        // 访问的域名
        config.endpoint = "mt.cn-hangzhou.aliyuncs.com";
        return new com.aliyun.alimt20181012.Client(config);
    }

    private String translate (String labelName)  throws Exception {
        if(StringUtils.isBlank(labelName)){
            return null;
        }
        //避免翻译太长的字符串，这个费钱，哈哈
        if(labelName.length()>32){
            labelName = labelName.substring(0,32);
        }
        com.aliyun.alimt20181012.Client client = this.createClient();
        com.aliyun.alimt20181012.models.TranslateGeneralRequest translateGeneralRequest
            = new com.aliyun.alimt20181012.models.TranslateGeneralRequest()
            .setFormatType("text")
            .setSourceLanguage("zh")
            .setTargetLanguage("en")
            .setSourceText(labelName)
            .setScene("general");
        com.aliyun.teautil.models.RuntimeOptions runtime = new com.aliyun.teautil.models.RuntimeOptions();
        //  try {
            // 复制代码运行请自行打印 API 的返回值
            TranslateGeneralResponse respond = client.translateGeneralWithOptions(translateGeneralRequest, runtime);
            String englishName =
                StringBaseOpt.castObjectToString(
                    respond.getBody().getData().toMap().get("Translated"));
            //System.out.println(englishName);
            int nPos = englishName.indexOf(';');
            if(nPos>0){
                englishName = englishName.substring(0,nPos);
            }
            return englishName;
        /*} catch (TeaException error) {
            // 如有需要，请打印 error
            com.aliyun.teautil.Common.assertAsString(error.message);
        } catch (Exception _error) {
            TeaException error = new TeaException(_error.getMessage(), _error);
            // 如有需要，请打印 error
            com.aliyun.teautil.Common.assertAsString(error.message);
        }*/
    }

    @Override
    public String transLabelToColumn(String labelName) {
        try {
            String english = translate(labelName);
            english = english.replaceAll(" ", "_");
            return english.toUpperCase(Locale.ROOT);
        } catch (Exception e){
            return null;
        }
    }

    @Override
    public String transLabelToProperty(String labelName) {
        String column = transLabelToColumn(labelName);
        return column==null? null : FieldType.mapPropName(column);
    }
}
