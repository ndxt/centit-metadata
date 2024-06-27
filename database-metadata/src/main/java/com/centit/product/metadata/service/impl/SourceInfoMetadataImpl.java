package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.SourceInfoMetadata;
import com.centit.product.metadata.service.PropertyConvertor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service("sourceInfoMetadata")
@Transactional
public class SourceInfoMetadataImpl implements SourceInfoMetadata {

    @Autowired
    private SourceInfoDao sourceInfoDao;

    private PropertyConvertor propertyConvertor;

    public SourceInfoMetadataImpl(){
        this.propertyConvertor = null;
    }

    public void setPropertyConvertor(PropertyConvertor propertyConvertor) {
        this.propertyConvertor = propertyConvertor;
    }

    @Override
    public SourceInfo fetchSourceInfo(String databaseCode) {
        SourceInfo sourceInfo = sourceInfoDao.getDatabaseInfoById(databaseCode);
        if(sourceInfo==null || this.propertyConvertor ==null)
            return sourceInfo;
        return this.propertyConvertor.convertSourceInfo(sourceInfo);
    }
}
