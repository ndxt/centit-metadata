package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.SourceInfoManager;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import com.centit.tenant.dubbo.adapter.TenantManageService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("databaseInfoManager")
@Transactional
public class SourceInfoManagerImpl implements SourceInfoManager {

    //private static final SysOptLog sysOptLog = SysOptLogFactoryImpl.getSysOptLog();

    //防止其他工程引用这个工程的同时没有配置dubbo客户端，导致启动报错
    @Autowired(required = false)
    private TenantManageService tenantManageService;

    @Autowired
    private SourceInfoDao sourceInfoDao;


    @Override
    public SourceInfo getObjectById(String databaseCode) {
        return sourceInfoDao.getDatabaseInfoById(databaseCode);
    }

    @Override
    public int deleteObjectById(String databaseCode) {
        return sourceInfoDao.deleteObjectById(databaseCode);
    }

    @Override
    public List<SourceInfo> listDatabase() {
        return sourceInfoDao.listDatabase();
    }

    @Override
    public void saveNewObject(SourceInfo sourceInfo) {
        checkDatabaseNumberLimit(sourceInfo);
        sourceInfoDao.saveNewObject(sourceInfo);
    }

    @Override
    public void mergeObject(SourceInfo sourceInfo){
        sourceInfoDao.mergeObject(sourceInfo);
    }

    @Override
    @Transactional(readOnly = true)
    public Map<String, SourceInfo> listDatabaseToDBRepo() {
        List<SourceInfo> dbList = sourceInfoDao.listObjects();
        Map<String, SourceInfo> dbmap = new HashMap<>();
        if(dbList != null){
            for(SourceInfo db : dbList){
                dbmap.put(db.getDatabaseCode(),db);
            }
        }
        return dbmap;
    }

    @Override
    public List<SourceInfo> listObjects(Map<String, Object> map){
        return sourceInfoDao.listObjectsByProperties(map);
    }

    @Override
    public JSONArray listDatabaseAsJson(Map<String, Object> filterMap, PageDesc pageDesc){
        return sourceInfoDao.listObjectsByPropertiesAsJson(filterMap, pageDesc);
    }

    @Override
    public JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc){
        return sourceInfoDao.queryDatabaseAsJson(databaseName, pageDesc);
    }

    @Override
    @Transactional
    public List<SourceInfo> listDatabaseByOsId(String osId) {
        return sourceInfoDao.listObjectsByProperties(
            CollectionsOpt.createHashMap("osId",osId));
    }


    /**
     * 验证资源数量是否达到限制
     * 目前只对数据库资源个数进行限制
     * @param sourceInfo
     */
    private void checkDatabaseNumberLimit(SourceInfo sourceInfo) {
        JSONObject tenantInfo = tenantManageService.getTenantInfoByTopUnit(sourceInfo.getTopUnit());
        if (null == tenantInfo){
            throw new ObjectException("租户信息有误!");
        }
        int countDataBase = countDataBase(sourceInfo.getTopUnit(), null);
        if (countDataBase >= tenantInfo.getIntValue("databaseNumberLimit")){
            throw new ObjectException("数据库资源超过了限制!");
        }
    }

    /**
     * 统计租户下数据个数
     * @param topUnit
     * @param sourceType
     * @return
     */
    private int countDataBase(String topUnit,String sourceType) {
        Map<String, Object> params = CollectionsOpt.createHashMap("topUnit", topUnit);
        if (StringUtils.isNotBlank(sourceType)){
            params.put("sourceType",sourceType);
        }
        return sourceInfoDao.countDataBase(params);
    }
}

