package com.centit.product.metadata.transaction;


import com.centit.product.metadata.api.ISourceInfo;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.network.HttpExecutor;
import com.centit.support.network.HttpExecutorContext;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


/**
 * @author zhf
 */
public abstract class AbstractHttpContextCreator {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHttpContextCreator.class);

    private AbstractHttpContextCreator() {
        throw new IllegalAccessError("Utility class");
    }

    private static void loginOpt(ISourceInfo dsDesc, HttpClientContext context, CloseableHttpClient httpClient) throws IOException {
        if (dsDesc.getExtProps() != null && dsDesc.getExtProps().get("loginUrl")!=null) {
            HttpExecutor.formPost(HttpExecutorContext.create(httpClient).context(context),
                (String) dsDesc.getExtProps().get("loginUrl"),
                dsDesc.getExtProps(), false);
        }
    }

    static synchronized HttpExecutorContext createHttpConnect(ISourceInfo dsDesc) throws Exception {
        Map<String, Object> extProps = dsDesc.getExtProps();
        if(extProps.containsKey("SSL") && BooleanBaseOpt.castObjectToBoolean(extProps.get("SSL"))){
            CloseableHttpClient keepSessionHttpsClient = HttpExecutor.createKeepSessionHttpsClient();
            return HttpExecutorContext.create(keepSessionHttpsClient);
        }else {
            HttpClientContext context = HttpClientContext.create();
            BasicCookieStore cookieStore = new BasicCookieStore();
            CloseableHttpClient httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
            loginOpt(dsDesc, context, httpClient);
            context.setCookieStore(cookieStore);
            String connection = StringBaseOpt.castObjectToString(dsDesc.getExtProps().get("Connection"),"close");
            int timeout = NumberBaseOpt.castObjectToInteger(dsDesc.getExtProps().get("timeout"), 60000);
            return HttpExecutorContext.create(httpClient).context(context).header("Connection", connection).timout(timeout);
        }
    }

}
