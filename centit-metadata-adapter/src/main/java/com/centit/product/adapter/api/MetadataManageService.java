package com.centit.product.adapter.api;

import java.util.Map;

public interface MetadataManageService {

    /**
     * 根据optId删除MetaOptRelation关联信息
     * @param optId
     * @return
     */
    int deleteMetaOptRelationByOptId(String optId);

    /**
     *
     * @param params
     * topUnit    租户code
     * sourceType 资源类型,D:关系数据库 M:MongoDb R:redis E:elssearch K:kafka B:rabbitmq,H http服务
     * @return
     */
    int countDataBase(Map<String, Object> params);
}
