package com.centit.product.metadata.controller;

import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.adapter.po.MetaOptRelation;
import com.centit.product.metadata.service.MetaOptRelationService;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@Api(value = "关联元数据表维护接口", tags = "关联元数据表维护接口")
@RestController
@RequestMapping(value = "/relation")
public class MetaOptRelationController {
    @Autowired
    private MetaOptRelationService metaOptRelationService;

    @ApiOperation(value = "新增版本信息")
    @PostMapping("/addOptRelation")
    @WrapUpResponseBody
    public void createMetaOptRelation(@RequestBody MetaOptRelation relation) {
        metaOptRelationService.createMetaOptRelation(relation);
    }

    @ApiOperation(value = "批量新增版本信息")
    @PostMapping("/batchAddOptRelation")
    @WrapUpResponseBody
    public void batchAddOptRelation(@RequestBody List<MetaOptRelation> relations) {
        metaOptRelationService.batchAddOptRelation(relations);
    }
    @ApiOperation(value = "编辑版本信息")
    @PutMapping(value = "/updateOptRelation")
    @WrapUpResponseBody
    public void updateMetaOptRelation(@RequestBody MetaOptRelation metaOptRelation) {
        metaOptRelationService.updateMetaOptRelation(metaOptRelation);
    }

    @ApiOperation(value = "删除版本信息")
    @DeleteMapping(value = "/{id}")
    @WrapUpResponseBody
    public void deleteMetaOptRelation(@PathVariable String id) {
        metaOptRelationService.deleteMetaOptRelation(id);
    }

    @ApiOperation(value = "查询版本信息列表")
    @GetMapping("/listOptRelation")
    @WrapUpResponseBody
    public PageQueryResult<MetaOptRelation> listHistory(HttpServletRequest request, PageDesc pageDesc) {
        List<MetaOptRelation> list = metaOptRelationService.listMetaOptRelation(BaseController.collectRequestParameters(request), pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个版本信息")
    @GetMapping(value = "/{tableId}")
    @WrapUpResponseBody
    public MetaOptRelation getMetaOptRelation(@PathVariable String tableId) {
        return metaOptRelationService.getMetaOptRelation(tableId);
    }
}
