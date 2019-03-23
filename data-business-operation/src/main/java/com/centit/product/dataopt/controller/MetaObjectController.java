package com.centit.product.dataopt.controller;

import com.centit.framework.core.controller.BaseController;
import com.centit.product.dataopt.service.MetaObjectService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 对数据库进行简单的增删改查，这个接口不能对外公开
 *
 * 对外公开的应该式自定义表单对应的额接口
 */
@RestController
@RequestMapping(value = "chart")
@Api(value = "基于元数据的数据访问服务", tags = "数据访问")
public class MetaObjectController extends BaseController {

    @Autowired
    private MetaObjectService metaObjectService;

}
