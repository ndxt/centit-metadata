package com.centit.product.dataopt.bizopt;

import com.centit.product.dataopt.core.BizModel;
import com.centit.product.dataopt.utils.BizOptUtils;
import com.centit.product.dataopt.utils.JSRuntimeContext;
import com.centit.product.metadata.service.DatabaseRunTime;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.common.ObjectException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.Map;

public class JSMateObjectEventRuntime {

    private static final Logger logger = LoggerFactory.getLogger(JSMateObjectEventRuntime.class);
    private MetaObjectService metaObjectService;
    private JSRuntimeContext jsRuntimeContext;

    public void setBizModel(BizModel bizModel) {
        this.bizModel = bizModel;
    }

    private BizModel bizModel;

    public Map<String, Object> getParms() {
        return parms;
    }

    public void setParms(Map<String, Object> parms) {
        this.parms = parms;
    }

    private Map<String, Object> parms;
    private DatabaseRunTime databaseRunTime;

    public String getJavaScript() {
        return javaScript;
    }

    public void setJavaScript(String javaScript) {
        this.javaScript = javaScript;
    }

    private String javaScript;


    public JSMateObjectEventRuntime(MetaObjectService metaObjectService,
                                    DatabaseRunTime databaseRunTime){
        this.metaObjectService = metaObjectService;
        this.databaseRunTime = databaseRunTime;
    }

    /**
     * 运行js事件
     */
    public BizModel runEvent()  {
        if(jsRuntimeContext == null){
            jsRuntimeContext = new JSRuntimeContext();
        }

        if(StringUtils.isNotBlank(javaScript)){
            jsRuntimeContext.compileScript(javaScript);
        }
        try {
            Object retObj = jsRuntimeContext.callJsFunc("runOpt", this, parms);
            return BizOptUtils.castObjectToBizModel(retObj);
        } catch (ScriptException e) {
            throw new ObjectException(ObjectException.UNKNOWN_EXCEPTION,
                    e.getMessage());
        } catch (NoSuchMethodException e) {
            logger.info(e.getMessage());
            return this.bizModel;
        }
    }

    public void setJsRuntimeContext(JSRuntimeContext jsRuntimeContext) {
        this.jsRuntimeContext = jsRuntimeContext;
    }

    public MetaObjectService getMetaObjectService() {
        return this.metaObjectService;
    }

    public DatabaseRunTime getDatabaseRunTime() {
        return this.databaseRunTime;
    }
    public BizModel getBizModel(){return  this.bizModel;}

}
