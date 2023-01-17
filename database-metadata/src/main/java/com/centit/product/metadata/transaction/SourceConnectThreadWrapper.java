package com.centit.product.metadata.transaction;

import com.centit.product.adapter.api.ISourceInfo;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.network.HttpExecutorContext;
import io.lettuce.core.api.StatefulRedisConnection;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhf
 */
class SourceConnectThreadWrapper implements Serializable {
    private final Map<ISourceInfo, Object> connectPools;

    SourceConnectThreadWrapper() {
        this.connectPools = new ConcurrentHashMap<>(4);
    }

    Connection fetchConnect(ISourceInfo description) throws SQLException {
        if (StringBaseOpt.isNvl(description.getSourceType()) || ISourceInfo.DATABASE.equals(description.getSourceType())) {
            Connection conn = (Connection) connectPools.get(description);
            if (conn == null ||conn.isClosed()) {
                conn = AbstractDruidConnectPools.getDbcpConnect(description);
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
        for (Map.Entry<ISourceInfo, Object> map : connectPools.entrySet()) {
            if (StringBaseOpt.isNvl(map.getKey().getSourceType()) || ISourceInfo.DATABASE.equals(map.getKey().getSourceType())) {
                Connection conn = (Connection) map.getValue();
                conn.commit();
            }
        }
    }

    void rollbackAllWork() throws SQLException {
        if (connectPools.size() == 0) {
            return;
        }
        for (Map.Entry<ISourceInfo, Object> map : connectPools.entrySet()) {
            if (StringBaseOpt.isNvl(map.getKey().getSourceType()) || ISourceInfo.DATABASE.equals(map.getKey().getSourceType())) {
                Connection conn = (Connection) map.getValue();
                conn.rollback();
            }
        }
    }

    void releaseAllConnect() {
        if (connectPools.size() == 0) {
            return;
        }
        for (Map.Entry<ISourceInfo, Object> map : connectPools.entrySet()) {
            if (StringBaseOpt.isNvl(map.getKey().getSourceType()) || ISourceInfo.DATABASE.equals(map.getKey().getSourceType())) {

                Connection conn = (Connection) map.getValue();
                AbstractDruidConnectPools.closeConnect(conn);
            } /*else if (ISourceInfo.HTTP.equals(map.getKey().getSourceType())) {
                AbstractHttpConnectPools.releaseHttp(map.getKey());
            } */ else if (ISourceInfo.REDIS.equals(map.getKey().getSourceType())) {
                StatefulRedisConnection<String, String> conn = (StatefulRedisConnection<String, String>) map.getValue();
                AbstractRedisConnectPools.closeConnect(conn);
            } else if (ISourceInfo.ES.equals(map.getKey().getSourceType())) {
                //释放ESClient
                RestHighLevelClient conn = (RestHighLevelClient) map.getValue();
                AbstractEsClientPools.returnClient(map.getKey(), conn);
            }
        }
        connectPools.clear();
    }
}
