package com.centit.product.metadata.transaction;

import com.centit.product.adapter.api.ISourceInfo;
import com.centit.search.service.ESServerConfig;
import com.centit.search.service.IndexerSearcherFactory;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author codefan@sina.com
 */
public abstract class AbstractEsClientPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEsClientPools.class);


    private AbstractEsClientPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static GenericObjectPool<RestHighLevelClient> fetchClientPool(ISourceInfo dsDesc) {
        ESServerConfig config = new ESServerConfig();
        String[] hostAndIp = dsDesc.getDatabaseUrl().split(":");
        config.setServerHostIp(hostAndIp[0]);
        if(hostAndIp.length>1) {
            config.setServerHostPort(hostAndIp[1]);//env.getProperty("elasticsearch.server.port"));
        } else {// 默认端口
            String port = StringBaseOpt.castObjectToString(dsDesc.getExtProp("port"), "9200");
            config.setServerHostPort(port);
        }
        //env.getProperty("elasticsearch.server.cluster")
        config.setClusterName(StringBaseOpt.castObjectToString(dsDesc.getExtProp("cluster")));
        config.setOsId(dsDesc.getOsId());
        config.setUsername(dsDesc.getUsername());
        config.setPassword(dsDesc.getClearPassword());
        //"elasticsearch.filter.minScore
        config.setMinScore(NumberBaseOpt.castObjectToFloat(dsDesc.getExtProp("minScore"), 0.5f));

        return IndexerSearcherFactory.obtainclientPool(config);
    }

    public static synchronized RestHighLevelClient fetchESClient(ISourceInfo dsDesc) throws Exception {
        GenericObjectPool<RestHighLevelClient> clientPool =  fetchClientPool(dsDesc);
        return clientPool.borrowObject();
    }

    public static void returnClient(ISourceInfo dsDesc , RestHighLevelClient client) {
        GenericObjectPool<RestHighLevelClient> clientPool =  fetchClientPool(dsDesc);
        clientPool.returnObject(client);
    }
}
