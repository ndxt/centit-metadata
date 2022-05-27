package com.centit.product.metadata.transaction;

import com.centit.product.adapter.api.ISourceInfo;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.network.HttpExecutorContext;

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

    HttpExecutorContext fetchHttpContext(ISourceInfo description) throws Exception {
        if (ISourceInfo.HTTP.equals(description.getSourceType())) {
            HttpExecutorContext conn = (HttpExecutorContext) connectPools.get(description);
            if (conn == null) {
                conn = AbstractHttpConnectPools.getHttpConnect(description);
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

            } else if (ISourceInfo.HTTP.equals(map.getKey().getSourceType())) {
                AbstractHttpConnectPools.releaseHttp(map.getKey());
            }
        }
        connectPools.clear();
    }
}

