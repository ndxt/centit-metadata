package com.centit.product.metadata.vo;

import com.alibaba.fastjson.annotation.JSONField;
import com.centit.support.database.metadata.IDatabaseInfo;
import com.centit.support.database.utils.DBType;

import java.util.Map;

/**
 * 数据库基本信息
 */
public interface ISourceInfo extends IDatabaseInfo {
    String DATABASE="D";
    String MONGO_DB="M";
    String REDIS="R";
    String ELS="E";
    String KAFKA="K";
    String RABBIT_MQ="B";
    String getSourceType();
}

