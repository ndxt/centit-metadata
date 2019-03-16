package com.centit.support.dataopt.bizopt;

import com.centit.support.dataopt.core.BizModel;
import com.centit.support.dataopt.core.BizOperation;
import com.centit.support.dataopt.utils.BizOptUtils;
import com.centit.support.dataopt.utils.JSRuntimeContext;
import org.apache.commons.lang3.StringUtils;

public class JSBizOperation implements BizOperation {

    protected String javaScript;
    protected String jsFuncName;
    protected JSRuntimeContext jsRuntimeContext;

    @Override
    public BizModel apply(BizModel bizModel) {
        if(jsRuntimeContext == null){
            jsRuntimeContext = new JSRuntimeContext();
        }
        if(StringUtils.isNotBlank(javaScript)){
            jsRuntimeContext.compileScript(javaScript);
        }
        Object object = jsRuntimeContext.callJsFunc(
            StringUtils.isBlank(jsFuncName)? "runOpt" : jsFuncName, bizModel);
        return BizOptUtils.castObjectToBizModel(object);
    }

    public void setJavaScript(String javaScript) {
        this.javaScript = javaScript;
    }

    public JSRuntimeContext getJsRuntimeContext() {
        return jsRuntimeContext;
    }

    public void setJsRuntimeContext(JSRuntimeContext jsRuntimeContext) {
        this.jsRuntimeContext = jsRuntimeContext;
    }

    public void setJsFuncName(String jsFuncName) {
        this.jsFuncName = jsFuncName;
    }
}
