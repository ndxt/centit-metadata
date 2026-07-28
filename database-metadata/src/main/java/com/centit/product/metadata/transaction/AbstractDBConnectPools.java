package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.DBType;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhf
 */
public abstract class AbstractDBConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDBConnectPools.class);

    /** connectionTimeout 保护上限：Hikari 的 borrow 循环在 elapsed >= connectionTimeout 时才抛超时，
     *  若被配成几十万毫秒（常见于把秒当毫秒），单次获取连接即可把业务线程挂死十几分钟。
     *  超过此值视为配置错误，夹紧到上限并告警。 */
    private static final int MAX_CONNECTION_TIMEOUT_MS = 60000;

    /** 以稳定的 databaseCode 作为池键。
     *  SourceInfo.equals/hashCode 包含 databaseUrl/username/password/extProps，若以整个 ISourceInfo 为键，
     *  配置变更后新对象与旧键不相等，refreshDataSource/delDataSource 会因 containsKey 不命中而跳过关闭，
     *  导致旧池永久滞留(R-01)。改用 databaseCode 后，同一数据源始终命中同一个池。 */
    private static final
    ConcurrentHashMap<String, HikariDataSource> DATABASE_SOURCE_POOLS
        = new ConcurrentHashMap<>();

    private AbstractDBConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static HikariDataSource createDataSource(ISourceInfo dsDesc) {
        HikariDataSource ds = new HikariDataSource();
        // poolName 便于日志/监控/JMX 定位到具体数据源(R-07)
        ds.setPoolName(dsDesc.getDatabaseCode());
        //ds.setConnectionErrorRetryAttempts(3);
        DBType dbType=DBType.mapDBType(dsDesc.getDatabaseUrl());

        ds.setDriverClassName(DBType.getDbDriver(dbType));
        ds.setUsername(dsDesc.getUsername());
        ds.setPassword(dsDesc.getClearPassword());

        int socketTimeoutMs = NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("socketTimeout"), 30000);
        int connectTimeoutMs = NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("jdbcConnectTimeout"), 5000);
        ds.setJdbcUrl(buildJdbcUrlWithTimeout(dsDesc.getDatabaseUrl(), dbType, socketTimeoutMs, connectTimeoutMs));
        if (dbType == DBType.Oracle || dbType == DBType.DM || dbType == DBType.Oscar) {
            ds.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(connectTimeoutMs));
            ds.addDataSourceProperty("oracle.jdbc.ReadTimeout", String.valueOf(socketTimeoutMs));
        }

        ds.setMaximumPoolSize(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxActive"), 50));
        // maxLifetime 默认 30 分钟(对齐 Hikari 官方默认)，过短(如 3 分钟)会导致连接频繁重建(R-07)。
        // 注意应小于数据库 wait_timeout，可在 extProps 中按需覆盖。
        ds.setMaxLifetime(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxLifetime"), 1800000));
        ds.setIdleTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("idleTimeout"), 600000));

        ds.setConnectionTimeout(resolveConnectionTimeout(dsDesc));

        ds.setMinimumIdle(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("minIdle"), 5));

        // 不再按 dbType 自动填默认 validationQuery：HikariCP 在未设置 connectionTestQuery 时
        // 会用 JDBC4 Connection.isValid(validationTimeout) 校验，这是官方推荐路径；且 isValid 通常
        // 能正确遵守 validationTimeout，避免在僵尸连接上靠 Statement.setQueryTimeout 限时被驱动忽略而挂死。
        // 仅当用户在 extProp 显式配置了 validationQuery 时才作为测试查询（给怪驱动留 override 口子）。
        String validationQuery = StringBaseOpt.castObjectToString(dsDesc.getExtProp("validationQuery"));
        ds.setValidationTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("validationTimeout"), 5000));

        boolean testWhileIdle = BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testWhileIdle"), true);

        if(testWhileIdle && StringUtils.isNotBlank(validationQuery)){
            ds.setConnectionTestQuery(validationQuery);
        }

        // 泄漏检测：连接借出超过该阈值未归还，Hikari 会打印借出调用栈，便于定位泄漏(R-07)。
        // 设为 0 可关闭；须 < maxLifetime 且 >= 2000ms。
        ds.setLeakDetectionThreshold(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("leakDetectionThreshold"), 60000));

        return ds;
    }

    /**
     * 解析 connectionTimeout，并对异常大的值做夹紧保护。
     */
    private static int resolveConnectionTimeout(ISourceInfo dsDesc) {
        int configured = NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("connectionTimeout"), 5000);
        if (configured > MAX_CONNECTION_TIMEOUT_MS) {
            logger.warn("数据源 [{}] 的 connectionTimeout 配置为 {}ms，超过保护上限 {}ms，已夹紧为上限。" +
                    "过大的 connectionTimeout 会让单次获取连接挂住线程数分钟，请检查是否把秒当成了毫秒。",
                dsDesc.getDatabaseCode(), configured, MAX_CONNECTION_TIMEOUT_MS);
            return MAX_CONNECTION_TIMEOUT_MS;
        }
        return configured;
    }

    /**
     * 根据 DBType 为 JDBC URL 追加 socket/connect 超时参数，防止连接校验在僵尸 TCP 上 hang 到
     * OS 层 TCP 重传超时（Linux 默认约 940 秒）。
     * <p>
     * MySQL/PostgreSQL/KingBase/SQLServer 通过 URL 参数设置；
     * Oracle/DM/Oscar 不支持 URL 参数，返回原始 URL，由调用方通过 addDataSourceProperty 设置。
     */
    private static String buildJdbcUrlWithTimeout(String jdbcUrl, DBType dbType,
                                                   int socketTimeoutMs, int connectTimeoutMs) {
        if (jdbcUrl == null || dbType == null) {
            return jdbcUrl;
        }
        switch (dbType) {
            case MySql:
                return appendUrlParam(jdbcUrl, false,
                    "connectTimeout", String.valueOf(connectTimeoutMs),
                    "socketTimeout", String.valueOf(socketTimeoutMs));
            case PostgreSql:
            case KingBase:
                // PostgreSQL / KingBase 超时单位为秒
                return appendUrlParam(jdbcUrl, false,
                    "connectTimeout", String.valueOf(connectTimeoutMs / 1000),
                    "socketTimeout", String.valueOf(socketTimeoutMs / 1000));
            case SqlServer:
                // SQL Server 用分号分隔，超时单位为秒
                return appendUrlParam(jdbcUrl, true,
                    "loginTimeout", String.valueOf(connectTimeoutMs / 1000),
                    "socketTimeout", String.valueOf(socketTimeoutMs / 1000));
            default:
                return jdbcUrl;
        }
    }

    /**
     * 为 JDBC URL 追加键值对参数，自动处理分隔符并跳过已存在的同名参数。
     *
     * @param url                 原始 JDBC URL
     * @param semicolonSeparated  true=分号分隔（SQL Server），false=问号/与号分隔（MySQL/PG）
     * @param kvPairs             key1, value1, key2, value2, ...
     */
    private static String appendUrlParam(String url, boolean semicolonSeparated, String... kvPairs) {
        StringBuilder sb = new StringBuilder(url);
        boolean hasQuery = url.contains("?");
        for (int i = 0; i < kvPairs.length; i += 2) {
            String key = kvPairs[i];
            String value = kvPairs[i + 1];
            if (url.contains(key + "=")) {
                continue;
            }
            if (semicolonSeparated) {
                sb.append(";").append(key).append("=").append(value);
            } else {
                sb.append(hasQuery ? "&" : "?").append(key).append("=").append(value);
                hasQuery = true;
            }
        }
        return sb.toString();
    }

    /**
     * 刷新数据源连接池：按 databaseCode 原子替换旧池并关闭。
     * 不再以整个 ISourceInfo 判断 containsKey——其多字段 equals 在配置变更后会不命中而漏关旧池(R-01)。
     */
    public static void refreshDataSource(ISourceInfo dsDesc) {
        String code = dsDesc.getDatabaseCode();
        if (DATABASE_SOURCE_POOLS.containsKey(code)) {
            HikariDataSource newDs = createDataSource(dsDesc);
            HikariDataSource oldDs = DATABASE_SOURCE_POOLS.put(code, newDs);
            closeQuietly(oldDs, code);
        }
    }

    /**
     * 删除数据源连接池：按 databaseCode 原子移除并关闭。
     * 使用 remove 原子返回旧值，避免 get/remove 两步间的竞态(R-01)。
     */
    public static void delDataSource(ISourceInfo dsDesc) {
        String code = dsDesc.getDatabaseCode();
        HikariDataSource oldDs = DATABASE_SOURCE_POOLS.remove(code);
        closeQuietly(oldDs, code);
    }

    public static Connection getDbcpConnect(ISourceInfo dsDesc) throws SQLException {
        String poolKey = dsDesc.getDatabaseCode();
        if (poolKey == null) {
            throw new SQLException("数据源 databaseCode 为空，无法获取连接池");
        }
        // 池键使用稳定的 databaseCode(R-01)；computeIfAbsent 保证池的懒创建线程安全，无需 synchronized 全局锁。
        // 之前的 synchronized 会让所有数据源、所有线程串行获取连接，一旦某个线程因池耗尽而
        // 阻塞（最长 connectionTimeout），其余线程全部被锁死，引发雪崩式超时。
        HikariDataSource ds = DATABASE_SOURCE_POOLS.computeIfAbsent(poolKey, k -> createDataSource(dsDesc));
        long start = System.currentTimeMillis();
        try {
            Connection conn = ds.getConnection();
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > 200) {
                HikariPoolMXBean mx = ds.getHikariPoolMXBean();
                logger.warn("获取连接较慢 [{}]: {}ms, active={}, idle={}, total={}",
                    dsDesc.getDatabaseCode(), elapsed,
                    mx.getActiveConnections(), mx.getIdleConnections(), mx.getTotalConnections());
            }
            try {
                conn.setAutoCommit(false);
            } catch (SQLException sqle) {
                // setAutoCommit 失败必须立即归还连接，否则池将连接计为 active 直到外部超时回收(R-05)
                closeConnect(conn);
                throw sqle;
            }
            return conn;
        } catch (SQLException e) {
            // 池耗尽或 autoCommit 失败时快速失败并打印池状态，便于定位是连接泄漏还是慢查询。
            logPoolStatus(dsDesc, ds, e);
            throw e;
        }
    }

    /**
     * 打印 HikariCP 池状态，用于连接获取失败时的诊断。
     */
    private static void logPoolStatus(ISourceInfo dsDesc, HikariDataSource ds, SQLException e) {
        try {
            if (ds.isClosed()) {
                logger.error("获取数据库连接失败，数据源 [{}] 的连接池已关闭: {}",
                    dsDesc.getDatabaseCode(), e.getMessage(), e);
                return;
            }
            // getHikariPoolMXBean 在池启动后才可用
            HikariPoolMXBean mxBean = ds.getHikariPoolMXBean();
            if (mxBean != null) {
                logger.error("获取数据库连接失败，数据源 [{}] 池状态: active={}, idle={}, 等待线程数={}, 总连接={}, max={}, 错误: {}",
                    dsDesc.getDatabaseCode(),
                    mxBean.getActiveConnections(), mxBean.getIdleConnections(),
                    mxBean.getThreadsAwaitingConnection(), mxBean.getTotalConnections(),
                    ds.getMaximumPoolSize(), e.getMessage(), e);
            } else {
                logger.error("获取数据库连接失败，数据源 [{}]: {}", dsDesc.getDatabaseCode(), e.getMessage(), e);
            }
        } catch (Exception ignore) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeConnect(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 关闭并清空全部连接池。供应用关闭(@PreDestroy)调用，避免热部署/上下文重启时
     * 遗留 Hikari housekeeper 线程与数据库会话(R-04)。逐池容错，一个失败不阻断其余。
     */
    public static void closeAllDataSources() {
        int count = DATABASE_SOURCE_POOLS.size();
        for (Map.Entry<String, HikariDataSource> entry : DATABASE_SOURCE_POOLS.entrySet()) {
            closeQuietly(entry.getValue(), entry.getKey());
        }
        DATABASE_SOURCE_POOLS.clear();
        logger.info("已关闭全部数据库连接池，共 {} 个", count);
    }

    /**
     * 关闭单个连接池，吞掉异常以便逐池清理时一个失败不阻断其余。
     */
    private static void closeQuietly(HikariDataSource ds, String databaseCode) {
        if (ds == null) {
            return;
        }
        try {
            ds.close();
        } catch (Exception e) {
            logger.error("关闭数据源 [{}] 连接池失败: {}", databaseCode, e.getMessage(), e);
        }
    }

    public static void testConnect(SourceInfo sourceInfo) throws SQLException {
        // 测试能否成功获取连接：获取成功即视为连通性正常，由 try-with-resources 自动关闭
        try (HikariDataSource ds = createDataSource(sourceInfo);
             Connection conn = ds.getConnection()) {
        }
    }

}
