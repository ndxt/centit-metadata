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

    public static void rollbackAll() throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        wrapper.rollbackAllWork();
    }

    public static void rollback(ISourceInfo description) throws SQLException {
        SourceConnectThreadWrapper wrapper = getConnectThreadWrapper();
        Connection conn = wrapper.fetchConnect(description);
        conn.rollback();
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

    /**
     * 请求级兜底清理：回滚当前线程上所有未提交的连接并归还连接池，然后清除 ThreadLocal。
     * 供 Servlet Filter 在请求结束时调用，防止业务代码漏调 commitAndRelease/rollbackAndRelease
     * 导致连接泄漏（连接以 active 状态挂在 ThreadLocal 上，HikariCP 无法回收）。
     */
    public static void cleanupCurrentThread() {
        threadLocal.remove();
    }
}
