package com.centit.product.metadata.transaction;



import com.centit.product.adapter.api.ISourceInfo;
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
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhf
 */
public abstract class AbstractHttpConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHttpConnectPools.class);
    private static final
    Map<ISourceInfo, HttpExecutorContext> HTTP_DATA_SOURCE_POOLS
        = new ConcurrentHashMap<>();

    private AbstractHttpConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static HttpExecutorContext mapHttpSource(ISourceInfo dsDesc) throws IOException {
        HttpClientContext context = HttpClientContext.create();
        BasicCookieStore cookieStore = new BasicCookieStore();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultCookieStore(cookieStore).build()) {
            loginOpt(dsDesc, context, httpClient);
            context.setCookieStore(cookieStore);
            return HttpExecutorContext.create(httpClient).context(context).header("Connection", "close").timout(5000);
        }
    }

    private static void loginOpt(ISourceInfo dsDesc, HttpClientContext context, CloseableHttpClient httpClient) throws IOException {
        if (dsDesc.getExtProps() != null && dsDesc.getExtProps().get("loginUrl")!=null) {
            HttpExecutor.formPost(HttpExecutorContext.create(httpClient).context(context),
                (String) dsDesc.getExtProps().get("loginUrl"),
                dsDesc.getExtProps(), false);
        }
    }

    static synchronized HttpExecutorContext getHttpConnect(ISourceInfo dsDesc) throws IOException {
        HttpExecutorContext ds = HTTP_DATA_SOURCE_POOLS.get(dsDesc);
        if (ds == null) {
            ds = mapHttpSource(dsDesc);
            HTTP_DATA_SOURCE_POOLS.put(dsDesc, ds);
        }
        return ds;
    }

    static void releaseHttp(ISourceInfo dsDesc) {
        HTTP_DATA_SOURCE_POOLS.remove(dsDesc);
    }
}
