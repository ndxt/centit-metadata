package com.centit.product.metadata.service;

import com.centit.product.metadata.po.SourceInfo;

public interface SourceInfoMetadata {

    SourceInfo fetchSourceInfo(String databaseCode);
}
