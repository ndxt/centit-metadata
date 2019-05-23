package com.centit.support;

import com.centit.framework.config.SpringSecurityDaoConfig;
import com.centit.framework.ip.app.config.IPOrStaticAppSystemBeanConfig;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.framework.ip.service.impl.JsonIntegrationEnvironment;
import com.centit.framework.jdbc.config.JdbcConfig;
import com.centit.framework.model.adapter.PlatformEnvironment;
import com.centit.framework.security.model.CentitUserDetailsService;
import com.centit.framework.security.model.StandardPasswordEncoderImpl;
import com.centit.framework.staticsystem.service.impl.JsonPlatformEnvironment;
import com.centit.framework.staticsystem.service.impl.UserDetailsServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.security.web.csrf.HttpSessionCsrfTokenRepository;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.zip.GZIPOutputStream;

/**
 * Created by codefan on 17-7-18.
 */
@Configuration
@ComponentScan(basePackages = {"com.centit"},
        excludeFilters = @ComponentScan.Filter(value = org.springframework.stereotype.Controller.class))
@PropertySource(value = "classpath:system.properties")
@Import(value = {JdbcConfig.class, SpringSecurityDaoConfig.class, IPOrStaticAppSystemBeanConfig.class})
public class ServiceConfig {
    @Value("${redis.home:127.0.0.1}")
    private String redisHost;

    @Value("${redis.port:6379}")
    private int redisPort;

    @Bean
    public JedisPool jedisPool(){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1024);
        config.setMaxIdle(200);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        return new JedisPool(config, redisHost, redisPort, 10000);
    }
/*    @Bean
    public IntegrationEnvironment integrationEnvironment(){
        return new JsonIntegrationEnvironment();
    }

    @Bean
    public CsrfTokenRepository csrfTokenRepository() {
        return new HttpSessionCsrfTokenRepository();
    }

    @Bean
    public PlatformEnvironment platformEnvironment(){
         platformEnvironment = new JsonPlatformEnvironment();
        return platformEnvironment;
    }

    @Bean
    public CentitUserDetailsService centitUserDetailsService(PlatformEnvironment platformEnvironment) {
        UserDetailsServiceImpl userDetailsService = new UserDetailsServiceImpl();
        userDetailsService.setPlatformEnvironment(platformEnvironment);
        return userDetailsService;
    }*/

    @Bean("passwordEncoder")
    public StandardPasswordEncoderImpl passwordEncoder() {
        return  new StandardPasswordEncoderImpl();
    }

}
