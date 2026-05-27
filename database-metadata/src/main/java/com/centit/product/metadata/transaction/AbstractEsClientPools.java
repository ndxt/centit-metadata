package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
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

    private static synchronized GenericObjectPool<RestHighLevelClient> fetchClientPool(ISourceInfo dsDesc, boolean createNew) {
        try {
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
            return IndexerSearcherFactory.obtainclientPool(config, createNew);
        } catch (Exception e) {
            logger.error("Failed to create Elasticsearch client pool for source: {}. Error: {}",
                dsDesc.getOsId(), e.getMessage(), e);
            return null;
        }
    }

    public static synchronized RestHighLevelClient fetchESClient(ISourceInfo dsDesc) throws Exception {
        try {
            GenericObjectPool<RestHighLevelClient> clientPool = fetchClientPool(dsDesc, true);
            if (clientPool == null) {
                throw new IllegalStateException("Failed to create Elasticsearch client pool for source: " + dsDesc.getOsId());
            }

            // 检查池中是否有可用对象或能否创建新对象
            if (clientPool.getNumActive() >= clientPool.getMaxTotal()) {
                logger.warn("Elasticsearch client pool is exhausted for source: {}. Active: {}, Max: {}",
                    dsDesc.getOsId(), clientPool.getNumActive(), clientPool.getMaxTotal());
            }

            return clientPool.borrowObject();
        } catch (Exception e) {
            logger.error("Failed to borrow Elasticsearch client from pool for source: {}. Error: {}",
                dsDesc.getOsId(), e.getMessage(), e);
            throw new Exception("Unable to get Elasticsearch client for source: " + dsDesc.getOsId() + ". Reason: " + e.getMessage(), e);
        }
    }

    public static synchronized void returnClient(ISourceInfo dsDesc , RestHighLevelClient client) {
        if (client == null) {
            return;
        }

        try {
            GenericObjectPool<RestHighLevelClient> clientPool = fetchClientPool(dsDesc, false);
            if(clientPool!=null) {
                //client.close();
                clientPool.returnObject(client);
            } else {
                logger.warn("Client pool not found for source: {}, closing client directly", dsDesc.getOsId());
                try {
                    client.close();
                } catch (Exception e) {
                    logger.error("Error closing Elasticsearch client for source: {}", dsDesc.getOsId(), e);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to return Elasticsearch client to pool for source: {}. Error: {}",
                dsDesc.getOsId(), e.getMessage(), e);
            // 如果返回失败，尝试直接关闭客户端
            try {
                client.close();
            } catch (Exception closeEx) {
                logger.error("Error closing Elasticsearch client after failed return for source: {}", dsDesc.getOsId(), closeEx);
            }
        }
    }
}
