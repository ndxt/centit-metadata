package com.centit.product.dataopt.dataset;

import com.alibaba.fastjson.JSONObject;
import com.centit.product.dataopt.core.DataSetReader;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.support.network.HttpExecutor;
import com.centit.support.network.HttpExecutorContext;
import lombok.Data;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
/**
 * @author zhf
 */
@Data
public class HttpDataSet implements DataSetReader {
    private String sUrl;
    @Override
    public SimpleDataSet load(Map<String, Object> params) {
        String sResult = "";
        try (CloseableHttpClient httpClient = HttpExecutor.createHttpClient()) {
            sResult=HttpExecutor.simpleGet(HttpExecutorContext.create(httpClient),sUrl,params);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SimpleDataSet dataSet = new SimpleDataSet();
        dataSet.setData((List) JSONObject.parseObject(sResult).getJSONObject("data").getJSONArray("objList"));
        return dataSet;
    }
}
