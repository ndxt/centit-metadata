package com.centit.product.metadata.controller;

import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.common.JsonResultUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.components.OperationLogCenter;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpContentType;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.product.adapter.po.SourceInfo;
import com.centit.product.metadata.service.SourceInfoManager;
import com.centit.product.metadata.transaction.AbstractDruidConnectPools;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.network.HtmlFormUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * @author zhf
 */
@Controller
@RequestMapping("/database")
@Api(tags = "数据库维护接口", value = "数据库维护接口")
public class SourceInfoController extends BaseController {

    @Autowired
    private SourceInfoManager databaseInfoMag;

    private String optId = "DATABASE";

    /**
     * 数据库维护接口
     *
     * @param pageDesc 分页对象信息
     * @param request  HttpServletRequest
     * @param response HttpServletResponse
     * @return PageQueryResult 分页查询结果
     */
    @ApiOperation(value = "所有数据库列表信息", notes = "所有数据库列表信息。增加databaseCode")
    @ApiImplicitParam(
        name = "pageDesc", value = "json格式，分页对象信息",
        paramType = "body", dataTypeClass = PageDesc.class)
    @RequestMapping(method = RequestMethod.GET)
    @WrapUpResponseBody(contentType = WrapUpContentType.BASE64)
    public PageQueryResult<Object> list(PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        //如果用户还未加入到任何租户，不进行数据查询操作
        if (StringUtils.isBlank(topUnit)){
            return PageQueryResult.createResult(Collections.emptyList(), pageDesc);
        }
        if (WebOptUtils.isTenantTopUnit(request)){
            searchColumn.put("topUnit", topUnit);
        }
        JSONArray listObjects = databaseInfoMag.listDatabaseAsJson(searchColumn, pageDesc);

        return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, SourceInfo.class);
    }

    /**
     * 新增数据库信息
     *
     * @param databaseinfo 数据库对象信息
     * @param request      HttpServletRequest
     * @param response     HttpServletResponse
     */
    @ApiOperation(value = "新增数据库信息", notes = "新增数据库信息。")
    @ApiImplicitParam(
        name = "databaseinfo", value = "json格式，数据库对象信息", required = true,
        paramType = "body", dataTypeClass = SourceInfo.class)
    @RequestMapping(method = {RequestMethod.POST})
    public void saveDatabaseInfo(@RequestBody SourceInfo databaseinfo,
                                 HttpServletRequest request, HttpServletResponse response) {
        if (StringUtils.isBlank(WebOptUtils.getCurrentUserCode(request))){
            throw new ObjectException(ResponseData.ERROR_USER_NOT_LOGIN,"您未登录!");
        }
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        if (StringUtils.isBlank(topUnit)){
            throw new ObjectException(ResponseData.ERROR_INTERNAL_SERVER_ERROR,"您还未加入租户!");
        }
        databaseinfo.setTopUnit(topUnit);
        databaseinfo.setDatabaseUrl(HtmlFormUtils.htmlString(databaseinfo.getDatabaseUrl()));
        //加密
        if (StringUtils.isNotBlank(databaseinfo.getPassword())) {
            databaseinfo.setPassword(databaseinfo.getPassword());
        }
        databaseinfo.setCreated(WebOptUtils.getCurrentUserCode(request));
        databaseInfoMag.saveNewObject(databaseinfo);

        JsonResultUtils.writeSingleDataJson(databaseinfo.getDatabaseCode(),response);


        /**********************log************************/
        OperationLogCenter.logNewObject(request, optId, databaseinfo.getDatabaseCode(), OperationLog.P_OPT_LOG_METHOD_C,
            "新增数据库", databaseinfo);
        /**********************log************************/
    }

    /**
     * 连接测试
     *
     * @param sourceInfo 数据库信息
     * @param response   返回
     */
    @ApiOperation(value = "数据库连接测试", notes = "数据库连接测试。")
    @ApiImplicitParam(
        name = "databaseinfo", value = "json格式，数据库对象信息", required = true,
        paramType = "body", dataTypeClass = SourceInfo.class)
    @RequestMapping(value = "/testConnect", method = {RequestMethod.POST})
    public void testConnect(@Valid SourceInfo sourceInfo, HttpServletResponse response) {
        sourceInfo.setDatabaseUrl(HtmlFormUtils.htmlString(sourceInfo.getDatabaseUrl()));
        if (sourceInfo.getDatabaseCode() != null) {
            SourceInfo dataBaseSourceInfo = databaseInfoMag.getObjectById(sourceInfo.getDatabaseCode());
            if (dataBaseSourceInfo != null) {
                sourceInfo.setExtProps(dataBaseSourceInfo.getExtProps());
            }
            if(StringBaseOpt.isNvl(sourceInfo.getPassword())){
                sourceInfo.setPassword(dataBaseSourceInfo.getPassword());
            }
        }
        try {
            AbstractDruidConnectPools.testConnect(sourceInfo);
            JsonResultUtils.writeSingleDataJson("连接测试成功", response);
        } catch (SQLException e) {
            JsonResultUtils.writeErrorMessageJson(e.getMessage(), response);
        }

    }

    /**
     * 修改数据库信息
     *
     * @param databaseCode 数据库代码
     * @param databaseinfo 修改数据库信息
     * @param request      HttpServletRequest
     * @param response     HttpServletResponse
     */
    @ApiOperation(value = "修改数据库信息", notes = "修改数据库信息。")
    @ApiImplicitParams({
        @ApiImplicitParam(
            name = "databaseCode", value = "数据库代码",
            required = true, paramType = "path", dataType = "String"),
        @ApiImplicitParam(
            name = "databaseinfo", value = "json格式，数据库对象信息", required = true,
            paramType = "body", dataTypeClass = SourceInfo.class)
    })
    @RequestMapping(value = "/{databaseCode}", method = {RequestMethod.PUT})
    public void updateDatabaseInfo(@PathVariable String databaseCode, @RequestBody SourceInfo databaseinfo,
                                   HttpServletRequest request, HttpServletResponse response) {
        databaseinfo.setDatabaseUrl(HtmlFormUtils.htmlString((databaseinfo.getDatabaseUrl())));
        SourceInfo temp = databaseInfoMag.getObjectById(databaseCode);
        if (StringUtils.isNotBlank(databaseinfo.getPassword())) {
            if (!databaseinfo.getPassword().equals(temp.getPassword())) {
                databaseinfo.setPassword(databaseinfo.getPassword());
            }
        }

        SourceInfo oldValue = new SourceInfo();
        BeanUtils.copyProperties(temp, oldValue);
        databaseInfoMag.mergeObject(databaseinfo);

        JsonResultUtils.writeBlankJson(response);

        /**********************log****************************/
        OperationLogCenter.logUpdateObject(request, optId, databaseCode, OperationLog.P_OPT_LOG_METHOD_U,
            "更新数据库信息", databaseinfo, oldValue);
        /**********************log****************************/
    }

    /**
     * 获取单个数据库信息
     *
     * @param databaseCode 数据库代码
     * @param response     HttpServletResponse
     */
    @ApiOperation(value = "获取单个数据库信息", notes = "获取单个数据库信息。")
    @ApiImplicitParam(
        name = "databaseCode", value = "数据库代码",
        required = true, paramType = "path", dataType = "String")
    @RequestMapping(value = "/{databaseCode}", method = {RequestMethod.GET})
    public void getDatabaseInhfo(@PathVariable String databaseCode, HttpServletResponse response) {
        SourceInfo sourceInfo = databaseInfoMag.getObjectById(databaseCode);

        JsonResultUtils.writeSingleDataJson(sourceInfo, response);
    }

    /**
     * 删除单个数据库信息
     *
     * @param databaseCode 数据库代码
     * @param request      HttpServletRequest
     * @param response     HttpServletResponse
     */
    @ApiOperation(value = "删除单个数据库信息", notes = "删除单个数据库信息。")
    @ApiImplicitParam(
        name = "databaseCode", value = "数据库代码",
        required = true, paramType = "path", dataType = "String")
    @RequestMapping(value = "/{databaseCode}", method = {RequestMethod.DELETE})
    public void deleteDatabase(@PathVariable String databaseCode,
                               HttpServletRequest request, HttpServletResponse response) {
        SourceInfo sourceInfo = databaseInfoMag.getObjectById(databaseCode);
        databaseInfoMag.deleteObjectById(databaseCode);
        JsonResultUtils.writeBlankJson(response);

        /******************************log********************************/
        OperationLogCenter.logDeleteObject(request, optId, databaseCode, OperationLog.P_OPT_LOG_METHOD_D,
            "删除数据库", sourceInfo);
        /******************************log********************************/
    }

}
