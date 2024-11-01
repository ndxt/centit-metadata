package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.PropertyConvertor;
import com.centit.product.metadata.service.SourceInfoMetadata;
import com.centit.support.common.CachedMap;
import com.centit.support.common.CachedObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service("sourceInfoMetadata")
@Transactional
public class SourceInfoMetadataImpl implements SourceInfoMetadata {

    @Autowired
    private SourceInfoDao sourceInfoDao;

    private PropertyConvertor propertyConvertor;

    private final CachedMap<String, SourceInfo> sourceInfoCache;

    public SourceInfoMetadataImpl(){
        this.propertyConvertor = null;
        this.sourceInfoCache = new CachedMap<>(this::loadSourceInfo, CachedObject.DEFAULT_REFRESH_PERIOD);
    }

    @Override
    public void setPropertyConvertor(PropertyConvertor propertyConvertor) {
        this.propertyConvertor = propertyConvertor;
    }

    protected SourceInfo loadSourceInfo(String databaseCode) {
        SourceInfo sourceInfo = sourceInfoDao.getDatabaseInfoById(databaseCode);
        if(this.propertyConvertor ==null || sourceInfo==null)
            return sourceInfo;
        return this.propertyConvertor.convertSourceInfo(sourceInfo);
    }

    @Override
    public SourceInfo fetchSourceInfo(String databaseCode) {
        return sourceInfoCache.getCachedValue(databaseCode);
    }

    @Override
    public SourceInfo convertorSourceInfo(SourceInfo sourceInfo){
        if(this.propertyConvertor ==null || sourceInfo==null)
            return sourceInfo;
        return this.propertyConvertor.convertSourceInfo(sourceInfo);
    }

    @Override
    public void refreshCache() {
        sourceInfoCache.evictCache();
    }

    @Override
    public void refreshCache(String databaseCode) {
        sourceInfoCache.evictIdentifiedCache(databaseCode);
    }
}
