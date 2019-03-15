package com.centit.product.dataopt.service;

import com.centit.support.dataopt.core.BizModel;
import java.util.Map;

public interface MetaObjectService {

    Map<String, Object> getObjectById(String tableId, Map<String, Object> pk);

    BizModel getObjectById(String tableId, Map<String, Object> pk, int withChildrenDeep);

    int saveObject(String tableId, Map<String, Object> object);

    int saveObject(String tableId, BizModel object);
}
