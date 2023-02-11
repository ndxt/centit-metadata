package com.centit.product.metadata.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.common.JsonResultUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.components.OperationLogCenter;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpContentType;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.product.adapter.po.DataCheckRule;
import com.centit.product.metadata.service.DataCheckRuleService;
import com.centit.product.metadata.utils.DataCheckResult;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.ObjectTranslate;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * @author tian_y
 */
@Api(value = "数据校验规则维护接口", tags = "数据校验规则维护接口")
@RestController
@RequestMapping(value = "dataCheckRule")
public class DataCheckRuleController extends BaseController {

    @Autowired
    private DataCheckRuleService dataCheckRuleService;

    private String optId = "DATACHECKRULE";

    @ApiOperation(value = "所有数据校验规则信息", notes = "所有数据校验规则信息")
    @ApiImplicitParam(
        name = "pageDesc", value = "json格式，分页对象信息",
        paramType = "body", dataTypeClass = PageDesc.class)
    @RequestMapping(method = RequestMethod.GET)
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public PageQueryResult<Object> list(PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        //如果用户还未加入到任何租户，则查询公用的数据校验规则
        if (StringUtils.isBlank(topUnit)) {
            searchColumn.put("topUnit", "system");
        }
        if (WebOptUtils.isTenantTopUnit(request)) {
            String[] strArry = new String[2];
            strArry[0] = topUnit;
            strArry[1] = "system";
            searchColumn.put("topUnit", strArry);
        }
        JSONArray listObjects = dataCheckRuleService.listObjectsByPropertiesAsJson(searchColumn, pageDesc);
        return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, new Class[]{DataCheckRule.class});
    }

    @ApiOperation(value = "当前租户的数据校验规则信息", notes = "当前租户的数据校验规则信息")
    @ApiImplicitParam(
        name = "pageDesc", value = "json格式，分页对象信息",
        paramType = "body", dataTypeClass = PageDesc.class)
    @RequestMapping(value = "/listByTopUnit", method = {RequestMethod.GET})
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public PageQueryResult<Object> listByTopUnit(PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        if (StringUtils.isBlank(topUnit)) {
            throw new ObjectException(ResponseData.ERROR_INTERNAL_SERVER_ERROR, "topUnit不能为空!");
        }
        searchColumn.put("topUnit", topUnit);
        JSONArray listObjects = dataCheckRuleService.listObjectsByPropertiesAsJson(searchColumn, pageDesc);
        return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, new Class[]{DataCheckRule.class});
    }

    /**
     * 新增数据校验规则
     *
     * @param dataCheckRule 数据校验规则信息
     * @param request       HttpServletRequest
     * @param response      HttpServletResponse
     */
    @ApiOperation(value = "新增数据校验规则信息", notes = "新增数据校验规则信息")
    @ApiImplicitParam(
        name = "dataCheckRule", value = "json格式，数据校验规则信息", required = true,
        paramType = "body", dataTypeClass = DataCheckRule.class)
    @RequestMapping(method = {RequestMethod.POST})
    public void saveDataCheckRule(@RequestBody DataCheckRule dataCheckRule,
                                  HttpServletRequest request, HttpServletResponse response) {
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        if (StringUtils.isBlank(dataCheckRule.getTopUnit())) {
            //若未加入到租户，则添加到system公用
            if (StringUtils.isBlank(topUnit)) {
                dataCheckRule.setTopUnit("system");
            }
            dataCheckRule.setTopUnit(topUnit);
        }
        dataCheckRuleService.saveNewObject(dataCheckRule);

        JsonResultUtils.writeSingleDataJson(dataCheckRule.getRuleId(), response);

        /**********************log************************/
        OperationLogCenter.logNewObject(request, optId, dataCheckRule.getRuleId(), OperationLog.P_OPT_LOG_METHOD_C,
            "新增数据校验规则", dataCheckRule);
        /**********************log************************/
    }

    /**
     * 修改数据校验规则
     *
     * @param ruleId        数据校验规则代码
     * @param dataCheckRule 修改数据校验规则信息
     * @param request       HttpServletRequest
     * @param response      HttpServletResponse
     */
    @ApiOperation(value = "修改数据校验规则信息", notes = "修改数据校验规则信息。")
    @ApiImplicitParams({
        @ApiImplicitParam(
            name = "ruleId", value = "数据校验规则代码",
            required = true, paramType = "path", dataType = "String"),
        @ApiImplicitParam(
            name = "dataCheckRule", value = "json格式，数据库对象信息", required = true,
            paramType = "body", dataTypeClass = DataCheckRule.class)
    })
    @RequestMapping(value = "/{ruleId}", method = {RequestMethod.PUT})
    public void updateDataCheckRule(@PathVariable String ruleId, @RequestBody DataCheckRule dataCheckRule,
                                    HttpServletRequest request, HttpServletResponse response) {
        DataCheckRule temp = dataCheckRuleService.getObjectById(ruleId);
        DataCheckRule oldValue = new DataCheckRule();
        BeanUtils.copyProperties(temp, oldValue);
        dataCheckRuleService.updateObject(dataCheckRule);

        JsonResultUtils.writeBlankJson(response);

        /**********************log****************************/
        OperationLogCenter.logUpdateObject(request, optId, ruleId, OperationLog.P_OPT_LOG_METHOD_U,
            "修改数据校验规则信息", dataCheckRule, oldValue);
        /**********************log****************************/
    }

    /*
     * 获取单个数据校验规则
     *
     * @param ruleId   数据校验规则代码
     * @param response HttpServletResponse
     */
    @ApiOperation(value = "获取单个数据校验规则", notes = "获取单个数据校验规则。")
    @ApiImplicitParam(
        name = "ruleId", value = "数据校验规则代码",
        required = true, paramType = "path", dataType = "String")
    @RequestMapping(value = "/{ruleId}", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public DataCheckRule getDataCheckRule(@PathVariable String ruleId, HttpServletResponse response) {
        return dataCheckRuleService.getObjectById(ruleId);
    }

    /**
     * 删除单个数据校验规则
     *
     * @param ruleId   数据库代码
     * @param request  HttpServletRequest
     * @param response HttpServletResponse
     */
    @ApiOperation(value = "删除单个数据校验规则", notes = "删除单个数据校验规则。")
    @ApiImplicitParam(
        name = "ruleId", value = "数据校验规则代码",
        required = true, paramType = "path", dataType = "String")
    @RequestMapping(value = "/{ruleId}", method = {RequestMethod.DELETE})
    public void deleteDataCheckRule(@PathVariable String ruleId,
                                    HttpServletRequest request, HttpServletResponse response) {
        DataCheckRule dataChdeckRule = dataCheckRuleService.getObjectById(ruleId);
        dataCheckRuleService.deleteObjectById(ruleId);
        JsonResultUtils.writeBlankJson(response);

        /******************************log********************************/
        OperationLogCenter.logDeleteObject(request, optId, ruleId, OperationLog.P_OPT_LOG_METHOD_D,
            "删除数据校验规则", dataChdeckRule);
        /******************************log********************************/
    }

    @PostMapping(value = "/testformula")
    @ApiOperation(value = "测试表达式")
    @WrapUpResponseBody
    public Object testFormula(@RequestBody JSONObject jsonObject) {
        VariableFormula variableFormula = new VariableFormula();
        variableFormula.setExtendFuncMap(DataCheckResult.extraFunc);
        variableFormula.setTrans(new ObjectTranslate(jsonObject.containsKey("jsonString")?jsonObject.get("jsonString"):""));
        variableFormula.setFormula(jsonObject.containsKey("formula")?jsonObject.getString("formula"):"");
        return variableFormula.calcFormula();
    }


}
