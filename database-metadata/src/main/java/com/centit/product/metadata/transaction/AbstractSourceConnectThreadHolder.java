package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
import com.centit.support.network.HttpExecutorContext;
import io.lettuce.core.api.StatefulRedisConnection;
import org.elasticsearch.client.RestHighLevelClient;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author zhf
 */
public abstract class AbstractSourceConnectThreadHolder {
    private static SourceConnectThreadLocal threadLocal = new SourceConnectThreadLocal();

    private AbstractSourceConnectThreadHolder() {
        super();
    }

    private static SourceConnectThreadWrapper getConnectThreadWrapper() {
        SourceConnectThreadWrapper wrapper = threadLocal.get();
        if (wrapper == null) {
            wrapper = new SourceConnectThreadWrapper();
            threadLocal.set(wrapper);
        }
        return wrapper;
    }

    public static Connection fetchConnect(ISourceInfo description) throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        return wrapper.fetchConnect(description);
    }

    public static StatefulRedisConnection<String, String> fetchRedisConnect(ISourceInfo description)  {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        return wrapper.fetchRedisClient(description);
    }

    public static HttpExecutorContext fetchHttpContext(ISourceInfo description) throws Exception {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        return wrapper.fetchHttpContext(description);
    }

    public static RestHighLevelClient fetchESClient(ISourceInfo description) throws Exception {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        return wrapper.fetchESClient(description);
    }

    public static void commitAndRelease() throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        try {
            wrapper.commitAllWork();
        } finally {
            wrapper.releaseAllConnect();
            threadLocal.superRemove();
        }
    }

    public static void commitAll() throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        wrapper.commitAllWork();
    }

    public static void commit(ISourceInfo description) throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        Connection conn = wrapper.fetchConnect(description);
        conn.commit();
    }

    public static void rollbackAndRelease() throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        try {
            wrapper.rollbackAllWork();
        } finally {
            wrapper.releaseAllConnect();
            threadLocal.superRemove();
        }
    }
}
