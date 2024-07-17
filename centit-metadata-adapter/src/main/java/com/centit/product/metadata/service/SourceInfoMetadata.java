package com.centit.product.metadata.service;

import com.centit.product.metadata.po.SourceInfo;

public interface SourceInfoMetadata {

    void setPropertyConvertor(PropertyConvertor propertyConvertor);

    SourceInfo fetchSourceInfo(String databaseCode);

    void refreshCache();

    void refreshCache(String databaseCode);
}
