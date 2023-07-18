package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.components.CodeRepositoryCache;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.common.CachedObject;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Repository("sourceInfoDao")
public class SourceInfoDaoImpl implements SourceInfoDao{

    @Value("${app.home:./}")
    private String appHome;

    public CachedObject<List<SourceInfo>> sourceInfoRepo  =
        new CachedObject<>(this::loadAllSourceInfo,
            CodeRepositoryCache.CACHE_EXPIRE_EVERY_DAY);

    private List<SourceInfo> loadAllSourceInfo(){
        String ruleFile = appHome + File.separator + "config" + File.separator +  "resources.json";
        try {
            JSONArray array = JSON.parseArray(new FileInputStream(ruleFile));
            return array.toJavaList(SourceInfo.class);
        } catch (IOException e){
            return null;
        }
    }

    @Override
    public List<SourceInfo> listDatabase() {
        return sourceInfoRepo.getCachedTarget();
    }

    @Override
    public List<SourceInfo> listDatabaseByOsId(String osId) {
        return sourceInfoRepo.getCachedTarget();
    }

    @Override
    public SourceInfo getDatabaseInfoById(String databaseCode) {
        List<SourceInfo> allDatabases = sourceInfoRepo.getCachedTarget();
        if(allDatabases!=null){
            for(SourceInfo database : allDatabases){
                if(StringUtils.equals(database.getDatabaseCode(), (String) databaseCode)){
                    return database;
                }
            }
        }
        return null;
    }


    @Override
    public String getNextKey() {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int countDataBase(Map<String, Object> params) {
        List<SourceInfo> allDatabases = sourceInfoRepo.getCachedTarget();
        if(allDatabases!=null){
            return allDatabases.size();
        }
        return 0;
    }

    @Override
    public List<SourceInfo> listObjectsByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> filterMap, PageDesc pageDesc) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public void saveNewObject(SourceInfo sourceInfo) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int mergeObject(SourceInfo sourceInfo) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int deleteObjectById(Object id) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<SourceInfo> listObjects() {
        return sourceInfoRepo.getCachedTarget();
    }
}
