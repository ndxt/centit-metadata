package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
import com.centit.search.service.ESServerConfig;
import com.centit.search.service.IndexerSearcherFactory;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author codefan@sina.com
 */
public abstract class AbstractEsClientPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEsClientPools.class);

    // 用于跟踪连接借用情况,检测连接泄露
    private static final ConcurrentHashMap<String, AtomicLong> BORROW_COUNT_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicLong> RETURN_COUNT_MAP = new ConcurrentHashMap<>();

    // 连接泄露检测阈值:借出未归还的数量超过此值时发出警告
    private static final int LEAK_THRESHOLD = 10;

    // 连接池使用率警告阈值(百分比)
    private static final double USAGE_WARNING_THRESHOLD = 0.8;
    private static final double USAGE_CRITICAL_THRESHOLD = 0.95;


    private AbstractEsClientPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static synchronized GenericObjectPool<RestHighLevelClient> fetchClientPool(ISourceInfo dsDesc, boolean createNew) {
        try {
            ESServerConfig config = new ESServerConfig();
            String[] hostAndIp = dsDesc.getDatabaseUrl().split(":");
            config.setServerHostIp(hostAndIp[0]);
            if (hostAndIp.length > 1) {
                config.setServerHostPort(hostAndIp[1]);//env.getProperty("elasticsearch.server.port"));
            } else {// 默认端口
                String port = StringBaseOpt.castObjectToString(dsDesc.getExtProp("port"), "9200");
                config.setServerHostPort(port);
            }
            //env.getProperty("elasticsearch.server.cluster")
            config.setClusterName(StringBaseOpt.castObjectToString(dsDesc.getExtProp("cluster")));
            config.setUsername(dsDesc.getUsername());
            config.setPassword(dsDesc.getClearPassword());
            //"elasticsearch.filter.minScore
            config.setMinScore(NumberBaseOpt.castObjectToFloat(dsDesc.getExtProp("minScore"), 0.5f));

            // 配置连接池参数
            GenericObjectPoolConfig<RestHighLevelClient> poolConfig = new GenericObjectPoolConfig<>();

            // 最大连接数 (默认 20)
            int maxTotal = NumberBaseOpt.castObjectToInteger(dsDesc.getExtProp("maxTotal"), 20);
            poolConfig.setMaxTotal(maxTotal);

            // 最大空闲连接数 (默认 10)
            int maxIdle = NumberBaseOpt.castObjectToInteger(dsDesc.getExtProp("maxIdle"), 10);
            poolConfig.setMaxIdle(maxIdle);

            // 最小空闲连接数 (默认 2)
            int minIdle = NumberBaseOpt.castObjectToInteger(dsDesc.getExtProp("minIdle"), 2);
            poolConfig.setMinIdle(minIdle);

            // 获取连接的最大等待时间，单位毫秒 (默认 3000ms)
            long maxWaitMillis = NumberBaseOpt.castObjectToLong(dsDesc.getExtProp("maxWaitMillis"), 3000L);
            poolConfig.setMaxWaitMillis(maxWaitMillis);

            // 连接是否在借出时进行有效性检查 (默认 false)
            boolean testOnBorrow = BooleanBaseOpt.castObjectToBoolean(dsDesc.getExtProp("testOnBorrow"), false);
            poolConfig.setTestOnBorrow(testOnBorrow);

            // 连接是否在归还时进行有效性检查 (默认 false)
            boolean testOnReturn = BooleanBaseOpt.castObjectToBoolean(dsDesc.getExtProp("testOnReturn"), false);
            poolConfig.setTestOnReturn(testOnReturn);

            // 连接是否在空闲时进行有效性检查 (默认 true)
            boolean testWhileIdle = BooleanBaseOpt.castObjectToBoolean(dsDesc.getExtProp("testWhileIdle"), true);
            poolConfig.setTestWhileIdle(testWhileIdle);

            // 空闲连接检查的时间间隔，单位毫秒 (默认 30000ms)
            long timeBetweenEvictionRunsMillis = NumberBaseOpt.castObjectToLong(
                dsDesc.getExtProp("timeBetweenEvictionRunsMillis"), 30000L);
            poolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);

            // 连接的最小空闲时间，单位毫秒 (默认 600000ms = 10分钟)
            long minEvictableIdleTimeMillis = NumberBaseOpt.castObjectToLong(
                dsDesc.getExtProp("minEvictableIdleTimeMillis"), 600000L);
            poolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

            logger.debug("Elasticsearch client pool config for source {}: maxTotal={}, maxIdle={}, minIdle={}, maxWaitMillis={}",
                dsDesc.getDatabaseName(), maxTotal, maxIdle, minIdle, maxWaitMillis);

            // 创建连接池
            GenericObjectPool<RestHighLevelClient> clientPool = IndexerSearcherFactory.obtainclientPool(config, createNew);

            // 如果池已创建且是新建的,尝试应用自定义配置
            if (clientPool != null && createNew) {
                try {
                    clientPool.setConfig(poolConfig);
                    logger.info("Successfully applied custom pool config for source: {}", dsDesc.getDatabaseName());
                } catch (Exception configEx) {
                    logger.warn("Failed to apply custom pool config for source: {}, using default config. Error: {}",
                        dsDesc.getDatabaseName(), configEx.getMessage());
                }
            }

            return clientPool;
        } catch (Exception e) {
            logger.error("Failed to create Elasticsearch client pool for source: {}. Error: {}",
                dsDesc.getDatabaseName(), e.getMessage(), e);
            return null;
        }
    }

    public static synchronized RestHighLevelClient fetchESClient(ISourceInfo dsDesc) throws Exception {
        String sourceId = dsDesc.getDatabaseCode();
        try {
            GenericObjectPool<RestHighLevelClient> clientPool = fetchClientPool(dsDesc, true);
            if (clientPool == null) {
                throw new IllegalStateException("Failed to create Elasticsearch client pool for source: " + sourceId);
            }

            // 详细的连接池状态检查
            checkPoolStatus(sourceId, clientPool);

            RestHighLevelClient client = clientPool.borrowObject();

            // 记录借用次数
            BORROW_COUNT_MAP.computeIfAbsent(sourceId, k -> new AtomicLong(0)).incrementAndGet();

            // 检测可能的连接泄露
            detectPotentialLeak(sourceId, clientPool);

            return client;
        } catch (Exception e) {
            logger.error("Failed to borrow Elasticsearch client from pool for source: {}. Error: {}",
                sourceId, e.getMessage(), e);
            throw new Exception("Unable to get Elasticsearch client for source: " + sourceId + ". Reason: " + e.getMessage(), e);
        }
    }

    /**
     * 检查连接池状态,监控使用情况
     */
    private static void checkPoolStatus(String sourceId, GenericObjectPool<RestHighLevelClient> clientPool) {
        int numActive = clientPool.getNumActive();
        int numIdle = clientPool.getNumIdle();
        int maxTotal = clientPool.getMaxTotal();
        long borrowedCount = BORROW_COUNT_MAP.getOrDefault(sourceId, new AtomicLong(0)).get();
        long returnedCount = RETURN_COUNT_MAP.getOrDefault(sourceId, new AtomicLong(0)).get();

        // 计算使用率
        double usageRate = maxTotal > 0 ? (double) numActive / maxTotal : 0;

        // 记录池状态(调试级别)
        logger.debug("ES Pool Status [{}]: Active={}, Idle={}, MaxTotal={}, Borrowed={}, Returned={}, Usage={:.1f}%",
            sourceId, numActive, numIdle, maxTotal, borrowedCount, returnedCount, usageRate * 100);

        // 检查是否达到警告阈值
        if (usageRate >= USAGE_CRITICAL_THRESHOLD) {
            logger.error("CRITICAL: ES client pool nearly exhausted for source: {}! Active: {}/{}, Usage: {:.1f}%, Waiters: {}",
                sourceId, numActive, maxTotal, usageRate * 100, clientPool.getNumWaiters());
        } else if (usageRate >= USAGE_WARNING_THRESHOLD) {
            logger.warn("WARNING: ES client pool usage is high for source: {}. Active: {}/{}, Usage: {:.1f}%, Waiters: {}",
                sourceId, numActive, maxTotal, usageRate * 100, clientPool.getNumWaiters());
        }

        // 检查是否有等待的线程
        if (clientPool.getNumWaiters() > 0) {
            logger.warn("ES pool has {} threads waiting for connection [source: {}]",
                clientPool.getNumWaiters(), sourceId);
        }
    }

    /**
     * 检测潜在的连接泄露
     */
    private static void detectPotentialLeak(String sourceId, GenericObjectPool<RestHighLevelClient> clientPool) {
        long borrowedCount = BORROW_COUNT_MAP.getOrDefault(sourceId, new AtomicLong(0)).get();
        long returnedCount = RETURN_COUNT_MAP.getOrDefault(sourceId, new AtomicLong(0)).get();
        long notReturned = borrowedCount - returnedCount;
        int numActive = clientPool.getNumActive();

        // 如果借出未归还的数量异常高,可能存在泄露
        if (notReturned > LEAK_THRESHOLD && notReturned > numActive * 1.5) {
            logger.error("POTENTIAL CONNECTION LEAK detected for source: {}! " +
                    "Borrowed: {}, Returned: {}, Not Returned: {}, Currently Active: {}",
                sourceId, borrowedCount, returnedCount, notReturned, numActive);
        }
    }

    public static synchronized void returnClient(ISourceInfo dsDesc, RestHighLevelClient client) {
        String sourceId = dsDesc.getDatabaseCode();
        if (client == null) {
            logger.warn("Attempted to return null client for source: {}", sourceId);
            return;
        }

        try {
            GenericObjectPool<RestHighLevelClient> clientPool = fetchClientPool(dsDesc, false);
            if (clientPool == null) {
                // 池不存在时尝试创建新池再归还，避免直接 close() 导致 I/O Reactor STOPPED
                logger.warn("Client pool not found for source: {}, trying to create new pool", sourceId);
                clientPool = fetchClientPool(dsDesc, true);
            }
            if (clientPool != null) {
                // 记录归还次数
                RETURN_COUNT_MAP.computeIfAbsent(sourceId, k -> new AtomicLong(0)).incrementAndGet();

                // 归还前检查池状态
                int beforeReturn = clientPool.getNumActive();

                clientPool.returnObject(client);

                // 记录归还成功
                logger.debug("Client returned to pool for source: {}. Active before: {}, after: {}",
                    sourceId, beforeReturn, clientPool.getNumActive());
            } else {
                logger.error("Unable to find or create ES client pool for source: {}. Client may leak.", sourceId);
            }
        } catch (Exception e) {
            logger.error("Failed to return Elasticsearch client to pool for source: {}. Error: {}",
                sourceId, e.getMessage(), e);
        }
    }

}
