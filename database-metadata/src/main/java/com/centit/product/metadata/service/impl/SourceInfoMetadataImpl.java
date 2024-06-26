package com.centit.product.metadata.service.impl;

import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.SourceInfoMetadata;
import com.centit.product.metadata.service.SourceInfoPretreatment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service("sourceInfoMetadata")
@Transactional
public class SourceInfoMetadataImpl implements SourceInfoMetadata {

    @Autowired
    private SourceInfoDao sourceInfoDao;

    private SourceInfoPretreatment sourceInfoPretreatment;

    public SourceInfoMetadataImpl(){
        this.sourceInfoPretreatment = null;
    }

    public void setSourceInfoPretreatment(SourceInfoPretreatment sourceInfoPretreatment) {
        this.sourceInfoPretreatment = sourceInfoPretreatment;
    }

    @Override
    public SourceInfo fetchSourceInfo(String databaseCode) {
        SourceInfo sourceInfo = sourceInfoDao.getDatabaseInfoById(databaseCode);
        if(sourceInfo==null || this.sourceInfoPretreatment==null)
            return sourceInfo;
        return this.sourceInfoPretreatment.pretreatment(sourceInfo);
    }
}
