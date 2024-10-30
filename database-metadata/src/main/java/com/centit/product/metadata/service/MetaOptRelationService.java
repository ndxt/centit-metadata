package com.centit.product.metadata.service;

import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

/**
 * 元数据与业务关联关系
 */
public interface MetaOptRelationService {
    /**
     * 创建关联关系
     * @param relation 关联信息
     */
    void createMetaOptRelation(MetaOptRelation relation);

    /**
     * 更新关联信息
     * @param relation  关联信息
     */
    void updateMetaOptRelation(MetaOptRelation relation);

    /**
     * 删除关联关系
     * @param relationId 关系id
     */
    void deleteMetaOptRelation(String relationId);

    /**
     * 查询元数据与业务的关联关系
     * @param params 筛选条件
     * @param pageDesc 分页信息
     * @return 关联关系列表
     */
    List<MetaOptRelation> listMetaOptRelation(Map<String, Object> params, PageDesc pageDesc);

    /**
     * 获取元数据关联关系
     * @param relationId 关系id
     * @return 关系信息
     */
    MetaOptRelation getMetaOptRelation(String relationId);

    MetaOptRelation getMetaOptRelation(String osId, String tableId);
    /**
     * 批量保存关联关系
     * @param relations 关联关系列表
     */
    void batchAddOptRelation(List<MetaOptRelation> relations);

}
