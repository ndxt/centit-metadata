package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
import com.centit.support.network.HttpExecutorContext;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.http.impl.client.CloseableHttpClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhf
 */
class SourceConnectThreadWrapper implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SourceConnectThreadWrapper.class);

    private final Map<ISourceInfo, Object> connectPools;

    SourceConnectThreadWrapper() {
        this.connectPools = new ConcurrentHashMap<>(4);
    }

    Connection fetchConnect(ISourceInfo description) throws SQLException {
        if (ISourceInfo.DATABASE.equals(description.getSourceType())) {
            Connection conn = (Connection) connectPools.get(description);
            if (conn == null || conn.isClosed()) {
                conn = AbstractDBConnectPools.getDbcpConnect(description);
                connectPools.put(description, conn);
            }
            return conn;
        }
        return null;
    }

    StatefulRedisConnection<String, String> fetchRedisClient(ISourceInfo description)  {
        if (ISourceInfo.REDIS.equals(description.getSourceType())) {
            StatefulRedisConnection<String, String> client =
                (StatefulRedisConnection<String, String>) connectPools.get(description);
            if (client == null) {
                client = AbstractRedisConnectPools.getRedisConnect(description);
                connectPools.put(description, client);
            }
            return client;
        }
        return null;
    }

    HttpExecutorContext fetchHttpContext(ISourceInfo description) throws Exception {
        if (ISourceInfo.HTTP.equals(description.getSourceType())) {
            HttpExecutorContext conn = (HttpExecutorContext) connectPools.get(description);
            if (conn == null) {
                conn = AbstractHttpContextCreator.createHttpConnect(description);
                connectPools.put(description, conn);
            }
            return conn;
        }
        return null;
    }

    RestHighLevelClient fetchESClient(ISourceInfo description) throws Exception {
        if (ISourceInfo.ES.equals(description.getSourceType())) {
            RestHighLevelClient conn = (RestHighLevelClient) connectPools.get(description);
            if (conn == null) {
                conn = AbstractEsClientPools.fetchESClient(description);
                connectPools.put(description, conn);
            }
            return conn;
        }
        return null;
    }

    void commitAllWork() throws SQLException {
        if (connectPools.size() == 0) {
            return;
        }
        // 逐连接提交：单条连接提交失败不应阻断其余连接提交，失败的事务由 releaseAllConnect 的防御性回滚兜底。
        for (Map.Entry<ISourceInfo, Object> map : connectPools.entrySet()) {
            if (ISourceInfo.DATABASE.equals(map.getKey().getSourceType())) {
                Connection conn = (Connection) map.getValue();
                try {
                    if (conn != null && !conn.isClosed()) {
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    logger.error("提交数据库连接失败，将继续处理其余连接: {}", ex.getMessage(), ex);
                }
            }
        }
    }

    void rollbackAllWork() throws SQLException {
        if (connectPools.size() == 0) {
            return;
        }
        // 逐连接回滚：单条连接回滚失败不应阻断其余连接回滚。
        for (Map.Entry<ISourceInfo, Object> map : connectPools.entrySet()) {
            if (ISourceInfo.DATABASE.equals(map.getKey().getSourceType())) {
                Connection conn = (Connection) map.getValue();
                try {
                    if (conn != null && !conn.isClosed()) {
                        conn.rollback();
                    }
                } catch (SQLException ex) {
                    logger.error("回滚数据库连接失败，将继续处理其余连接: {}", ex.getMessage(), ex);
                }
            }
        }
    }

    void releaseAllConnect() {
        if (connectPools.size() == 0) {
            return;
        }
        for (Map.Entry<ISourceInfo, Object> map : connectPools.entrySet()) {
            String sourceType = map.getKey().getSourceType();
            if (ISourceInfo.DATABASE.equals(sourceType)) {
                Connection conn = (Connection) map.getValue();
                // 归还前防御性回滚未提交事务，避免 DB 端 IDLE IN TRANSACTION 残留锁；
                // Hikari 归还时也会回滚，此处显式做更稳妥。
                safeRollback(conn);
                AbstractDBConnectPools.closeConnect(conn);
            } else if (ISourceInfo.HTTP.equals(sourceType)) {
                // 每次 fetch 都新建了 CloseableHttpClient + PoolingHttpClientConnectionManager，必须关闭，
                // 否则泄漏 TCP socket 与连接管理器(含后台 evictor 线程)。
                HttpExecutorContext httpContext = (HttpExecutorContext) map.getValue();
                if (httpContext != null) {
                    CloseableHttpClient httpClient = httpContext.getHttpclient();
                    if (httpClient != null) {
                        try {
                            httpClient.close();
                        } catch (IOException ex) {
                            logger.error("关闭 HTTP 客户端失败: {}", ex.getMessage(), ex);
                        }
                    }
                }
            } else if (ISourceInfo.REDIS.equals(sourceType)) {
                StatefulRedisConnection<String, String> conn = (StatefulRedisConnection<String, String>) map.getValue();
                AbstractRedisConnectPools.closeConnect(conn);
            } else if (ISourceInfo.ES.equals(sourceType)) {
                //释放ESClient
                RestHighLevelClient conn = (RestHighLevelClient) map.getValue();
                AbstractEsClientPools.returnClient(map.getKey(), conn);
            } else {
                logger.warn("releaseAllConnect 遇到未识别的 sourceType [{}]，关联资源未被释放，请检查", sourceType);
            }
        }
        connectPools.clear();
    }

    /**
     * 防御性回滚：仅尝试回滚未提交事务，失败一律忽略(由 close / Hikari 归还兜底)，不阻断连接归还。
     */
    private static void safeRollback(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            if (!conn.isClosed()) {
                conn.rollback();
            }
        } catch (SQLException ignore) {
            // 回滚失败不阻断归还
        }
    }
}
