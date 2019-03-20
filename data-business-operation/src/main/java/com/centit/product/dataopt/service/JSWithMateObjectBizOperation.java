package com.centit.product.dataopt.service;

import com.centit.support.dataopt.bizopt.JSBizOperation;
import com.centit.support.dataopt.core.BizModel;
import com.centit.support.dataopt.utils.BizOptUtils;
import com.centit.support.dataopt.utils.JSRuntimeContext;
import org.apache.commons.lang3.StringUtils;

public class JSWithMateObjectBizOperation extends JSBizOperation {

    private MetaObjectService metaObjectService;
    @Override
    public BizModel apply(BizModel bizModel) {
        if(jsRuntimeContext == null){
            jsRuntimeContext = new JSRuntimeContext();
        }

        if(StringUtils.isNotBlank(javaScript)){
            jsRuntimeContext.compileScript(javaScript);
        }

        Object object = jsRuntimeContext.callJsFunc(
            StringUtils.isBlank(jsFuncName)? "runOpt" : jsFuncName, metaObjectService, bizModel);

        return BizOptUtils.castObjectToBizModel(object);
    }

    public void setMetaObjectService(MetaObjectService metaObjectService) {
        this.metaObjectService = metaObjectService;
    }
}