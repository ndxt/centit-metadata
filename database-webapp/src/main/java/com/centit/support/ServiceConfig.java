package com.centit.support;

import com.centit.framework.components.impl.NotificationCenterImpl;
import com.centit.framework.config.SpringSecurityDaoConfig;
import com.centit.framework.ip.app.config.IPOrStaticAppSystemBeanConfig;
import com.centit.framework.jdbc.config.JdbcConfig;
import com.centit.framework.model.adapter.NotificationCenter;
import com.centit.framework.security.model.StandardPasswordEncoderImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by codefan on 17-7-18.
 */
@Configuration
@ComponentScan(basePackages = {"com.centit"},
        excludeFilters = @ComponentScan.Filter(value = org.springframework.stereotype.Controller.class))
@PropertySource(value = "classpath:system.properties")
@Import(value = {JdbcConfig.class, SpringSecurityDaoConfig.class,
    IPOrStaticAppSystemBeanConfig.class})
public class ServiceConfig {

    @Value("${datapacket.buff.enabled:false}")
    private boolean dataBuffEnable;

    @Value("${redis.home:127.0.0.1}")
    private String redisHost;

    @Value("${redis.port:6379}")
    private int redisPort;

    @Bean
    public JedisPool jedisPool(){
        if(!dataBuffEnable){
            return null;
        }
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1024);
        config.setMaxIdle(200);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        return new JedisPool(config, redisHost, redisPort, 10000);
    }

    @Bean
    public NotificationCenter notificationCenter() {
        NotificationCenterImpl notificationCenter = new NotificationCenterImpl();
        notificationCenter.initDummyMsgSenders();
        //notificationCenter.registerMessageSender("innerMsg",innerMessageManager);
        return notificationCenter;
    }
    @Bean("passwordEncoder")
    public StandardPasswordEncoderImpl passwordEncoder() {
        return  new StandardPasswordEncoderImpl();
    }

}
