package com.centit.product.dubbo.adapter;

public interface MetaOptRelationService {
    /**
     * 根据optId批量删除MetaOptRelation关联信息
     * @param optIds 如果存在多个optId，用英文都好拼接。
     *               如：optId1,optId2
     * @return 删除数据的格式
     */
    int deleteMetaOptRelationByOptIds(String optIds);
}
