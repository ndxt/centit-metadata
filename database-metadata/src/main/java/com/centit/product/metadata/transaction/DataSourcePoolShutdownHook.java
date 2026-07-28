package com.centit.product.metadata.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

/**
 * 应用关闭时统一关闭所有静态 Hikari 连接池(R-04)。
 * <p>
 * AbstractDBConnectPools 是静态工具类，不受 Spring 生命周期管理；其连接池存储在静态 Map 中，
 * 应用上下文关闭(尤其是热部署/模块重载/共享 JVM)时不会自动关闭，会遗留 Hikari housekeeper 线程、
 * 数据库会话与类加载器引用。本组件通过 @PreDestroy 在上下文关闭时调用 closeAllDataSources() 兜底清理。
 *
 * @author zhf
 */
@Component
public class DataSourcePoolShutdownHook {
    private static final Logger logger = LoggerFactory.getLogger(DataSourcePoolShutdownHook.class);

    @PreDestroy
    public void shutdown() {
        logger.info("应用关闭，开始清理数据库连接池");
        AbstractDBConnectPools.closeAllDataSources();
    }
}
