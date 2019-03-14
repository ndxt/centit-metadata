package com.centit.support.dataopt.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.File;
import java.io.FileReader;

public class JSRuntimeContext {
    protected static final Logger logger = LoggerFactory.getLogger(JSRuntimeContext.class);
    private ScriptEngine scriptEngine;

    public JSRuntimeContext(){
        ScriptEngineManager sem = new ScriptEngineManager();
        ScriptEngine scriptEngine = sem.getEngineByName("js"); // "nashorn" 等价与 “js”, "JavaScript"
    }

    public JSRuntimeContext compileScript(String js){
        try {
            scriptEngine.eval(js);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return this;
    }

    public JSRuntimeContext compileScriptFile(String jsFileName){
        try {
            FileReader reader = new FileReader(new File(jsFileName));
            scriptEngine.eval(reader);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return this;
    }

    public Object callJsFunc(String funcName, Object... args){
        try {
            Invocable invocable = (Invocable) scriptEngine;
            return invocable.invokeFunction(funcName, args);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return null;
        }
    }

    public Object getJsObject(String objName){
        try {
            return scriptEngine.get(objName);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return null;
        }
    }

    public Object getJsObjectProperty(String objName, String propertyName){
        try {
            return scriptEngine.eval(objName+"."+propertyName);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return null;
        }
    }

    public Object callJsObjectMethod(Object jsObject, String methodName, Object... args){
        try {
            Invocable invocable = (Invocable) scriptEngine;
            return invocable.invokeMethod(jsObject, methodName, args);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return null;
        }
    }
}
