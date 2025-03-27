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
import com.centit.framework.model.adapter.PlatformEnvironment;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.model.basedata.OsInfo;
import com.centit.framework.model.basedata.WorkGroup;
import com.centit.product.metadata.api.ISourceInfo;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.SourceInfoManager;
import com.centit.product.metadata.service.SourceInfoMetadata;
import com.centit.product.metadata.transaction.AbstractDBConnectPools;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.network.HtmlFormUtils;
import com.centit.support.network.HttpExecutor;
import com.centit.support.network.HttpExecutorContext;
import com.centit.support.network.SoapWsdlParser;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author zhf
 */
@Controller
@RequestMapping("/database")
@Api(tags = "数据库维护接口", value = "数据库维护接口")
public class SourceInfoController extends BaseController {

    @Autowired
    private SourceInfoManager databaseInfoMag;

    @Autowired
    private SourceInfoMetadata sourceInfoMetadata;
    @Value("${os.file.base.dir:./}")
    private String osFileDir;
    @Autowired
    private PlatformEnvironment platformEnvironment;

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
        paramType = "query", dataTypeClass = PageDesc.class)
    @RequestMapping(method = RequestMethod.GET)
    @WrapUpResponseBody(contentType = WrapUpContentType.BASE64)
    public PageQueryResult<Object> list(PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        //如果用户还未加入到任何租户，不进行数据查询操作
        if (StringUtils.isBlank(topUnit)){
            return PageQueryResult.createResult(Collections.emptyList(), pageDesc);
        }
        searchColumn.put("topUnit", topUnit);
        JSONArray listObjects = databaseInfoMag.listDatabaseAsJson(searchColumn, pageDesc);
        //附加关联的应用信息
        databaseInfoMag.appendRelativeOsInfo(listObjects);
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
        // sourceInfoMetadata.refreshCache(databaseinfo.getDatabaseCode());
        JsonResultUtils.writeSingleDataJson(databaseinfo.getDatabaseCode(),response);

        /**********************log************************/
        OperationLogCenter.logNewObject(request, optId, databaseinfo.getDatabaseCode(), OperationLog.P_OPT_LOG_METHOD_C,
            "新增数据库", databaseinfo);
        /**********************log************************/
    }
    /**
     * 新增数据库信息
     *
     * @param osId 应用id
     * @param request      HttpServletRequest
     * @param response     HttpServletResponse
     */
    @ApiOperation(value = "新增文件数据库", notes = "新增文件数据库。")
    @ApiImplicitParam(
        name = "h2", value = "json格式，数据库对象信息", required = true,
        paramType = "body", dataTypeClass = SourceInfo.class)
    @RequestMapping(value="/createH2",method = {RequestMethod.POST})
    public void createH2DatabaseInfo(String osId,
                                 HttpServletRequest request, HttpServletResponse response) throws IOException {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if (StringUtils.isBlank(userCode)){
            throw new ObjectException(ResponseData.ERROR_USER_NOT_LOGIN,"您未登录!");
        }
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        if (StringUtils.isBlank(topUnit)){
            throw new ObjectException(ResponseData.ERROR_INTERNAL_SERVER_ERROR,"您还未加入租户!");
        }
        if (StringUtils.isBlank(osId)){
            throw new ObjectException(ResponseData.ERROR_INTERNAL_SERVER_ERROR,"缺少应用相关的参数!");
        }
        List<WorkGroup> userGroups = platformEnvironment.listWorkGroup(osId, userCode, null);
        if (userGroups == null || CollectionUtils.isEmpty(userGroups)) {
            throw new ObjectException(ResponseData.HTTP_NON_AUTHORITATIVE_INFORMATION, "您不在该应用开发组中！");
        }

        if(databaseInfoMag.getObjectById(osId)!=null){
            throw new ObjectException(ResponseData.ERROR_INTERNAL_SERVER_ERROR,"此应用已创建过文件数据库，不能再次创建!");
        }
        String url="";
        if(osFileDir.endsWith("/") || osFileDir.endsWith("\\")) {
            url = osFileDir + "h2/" + osId;
        } else {
            url = osFileDir + File.separatorChar + "h2/" + osId;
        }
        url = "jdbc:h2:file:"+url;
        SourceInfo databaseinfo = new SourceInfo();
        databaseinfo.setTopUnit(topUnit);
        databaseinfo.setDatabaseUrl(url);
        OsInfo osInfo = platformEnvironment.getOsInfo(osId);
        if(osInfo!=null){
            databaseinfo.setDatabaseName(osInfo.getOsName()+"【内置文件数据库】");
        }else {
            databaseinfo.setDatabaseName(osId);
        }
        databaseinfo.setSourceType("D");
        databaseinfo.setCreated(userCode);
        databaseinfo.setDatabaseCode(osId);
        try {
            AbstractDBConnectPools.testConnect(databaseinfo);
            databaseInfoMag.saveNewObject(databaseinfo);
            JsonResultUtils.writeSingleDataJson(databaseinfo.getDatabaseCode(),response);
        } catch (SQLException e) {
            JsonResultUtils.writeErrorMessageJson(e.getMessage(), response);
        }
        OperationLogCenter.logNewObject(request, optId, databaseinfo.getDatabaseCode(), OperationLog.P_OPT_LOG_METHOD_C,
            "新增数据库", databaseinfo);
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
        sourceInfo = sourceInfoMetadata.convertorSourceInfo(sourceInfo);

        if (sourceInfo.getDatabaseCode() != null) {
            sourceInfoMetadata.refreshCache(sourceInfo.getDatabaseCode());
            SourceInfo dataBaseSourceInfo = sourceInfoMetadata.fetchSourceInfo(sourceInfo.getDatabaseCode());
            /*if (dataBaseSourceInfo != null) {
                sourceInfo.setExtProps(dataBaseSourceInfo.getExtProps());
            }*/
            if(StringUtils.isBlank(sourceInfo.getPassword())){
                sourceInfo.setPassword(dataBaseSourceInfo.getPassword());
            }
        }
        try {
            AbstractDBConnectPools.testConnect(sourceInfo);
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
                // 更新时确保密码正确
                databaseinfo.setPassword(databaseinfo.getPassword());
                try {
                    AbstractDBConnectPools.testConnect(databaseinfo);
                } catch (SQLException e) {
                    throw new ObjectException(ObjectException.SYSTEM_CONFIG_ERROR,
                        "数据库链接测试失败："+ e.getMessage(), e);
                }
            }
        }

        databaseInfoMag.mergeObject(databaseinfo);
        sourceInfoMetadata.refreshCache(databaseinfo.getDatabaseCode());
        //刷新连接池
        if(ISourceInfo.DATABASE.equals(databaseinfo.getSourceType())) {
            AbstractDBConnectPools.refreshDataSource(databaseinfo);
        }
        JsonResultUtils.writeBlankJson(response);
        /**********************log****************************/
        OperationLogCenter.logUpdateObject(request, optId, databaseCode, OperationLog.P_OPT_LOG_METHOD_U,
            "更新数据库信息", databaseinfo, temp);
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
        if(ISourceInfo.DATABASE.equals(sourceInfo.getSourceType())) {
            AbstractDBConnectPools.delDataSource(sourceInfo);
        }
        JsonResultUtils.writeBlankJson(response);

        /******************************log********************************/
        OperationLogCenter.logDeleteObject(request, optId, databaseCode, OperationLog.P_OPT_LOG_METHOD_D,
            "删除数据库", sourceInfo);
        /******************************log********************************/
    }

    @ApiOperation(value = "获取SOAP的 namespace", notes = "根据url获取soap的namespace")
    @ApiImplicitParam(
        name = "url", value = "Http服务的url 仅限于soap协议",
        paramType = "query", dataTypeClass = PageDesc.class)
    @RequestMapping(path = "soapnamespace", method = RequestMethod.GET)
    @WrapUpResponseBody
    public String getSoapNameSpace(String url) {
        try {
            String wsdl = HttpExecutor.simpleGet(HttpExecutorContext.create(), url + "?wsdl");
            Document doc = DocumentHelper.parseText(wsdl);
            return SoapWsdlParser.getSoapNameSpace(doc.getRootElement());
        } catch (IOException| DocumentException e) {
            return "error:" + e.getMessage();
        }
    }

    @ApiOperation(value = "获取SOAP的方法列表", notes = "根据服务id（databaseCode）获取soap的方法类别")
    @ApiImplicitParam(
        name = "httpServicesId", value = "Http服务的资源id：databaseCode",
        paramType = "query", dataTypeClass = PageDesc.class)
    @RequestMapping(path = "soapactions", method = RequestMethod.GET)
    @WrapUpResponseBody
    public List<String> getSoapActionList(String httpServicesId, String withInputName) {
        List<String> methods = new ArrayList<>();
        SourceInfo sourceInfo = databaseInfoMag.getObjectById(httpServicesId);
        if(sourceInfo == null) return methods;
        try {
            String wsdl = HttpExecutor.simpleGet(HttpExecutorContext.create(), sourceInfo.getDatabaseUrl() + "?wsdl");
            Document doc = DocumentHelper.parseText(wsdl);
            List<String> actiontsName = SoapWsdlParser.getSoapActionList(doc.getRootElement());
            if(BooleanBaseOpt.castObjectToBoolean(withInputName, false)){
                List<String> methodWithInputName = new ArrayList<>();
                for(String actName : actiontsName){
                    methodWithInputName.add(actName + ":" +
                        SoapWsdlParser.getSoapActionInputName(doc.getRootElement(), actName ));
                }
                return methodWithInputName;
            }
            return actiontsName;
        } catch (IOException| DocumentException e) {
            logger.error(e.getMessage());
        }
        return methods;
    }

    @ApiOperation(value = "获取SOAP的 方法参数", notes = "根据url获取soap的namespace")
    @ApiImplicitParams({
    @ApiImplicitParam(
        name = "httpServicesId", value = "Http服务的资源id：databaseCode",
        paramType = "query", dataTypeClass = PageDesc.class),
    @ApiImplicitParam(
        name = "httpServicesId", value = "Http服务的资源id：databaseCode",
        paramType = "query", dataTypeClass = PageDesc.class) })
    @RequestMapping(path = "soapactionparams", method = RequestMethod.GET)
    @WrapUpResponseBody
    public Map<String, String> getSoapActionParams(String httpServicesId, String actionName) {
        Map<String, String> params = new HashMap<>();
        SourceInfo sourceInfo = databaseInfoMag.getObjectById(httpServicesId);
        if(sourceInfo == null) return params;
        try {
            String wsdl = HttpExecutor.simpleGet(HttpExecutorContext.create(),
                sourceInfo.getDatabaseUrl() + "?wsdl");
            Document doc = DocumentHelper.parseText(wsdl);
            int p = actionName.indexOf(":");
            if(p>0){
                actionName = actionName.substring(0, p);
            }
            return SoapWsdlParser.getSoapActionParams(doc.getRootElement(), actionName);
        } catch (IOException| DocumentException e) {
            logger.error(e.getMessage());
        }
        return params;
    }
}
