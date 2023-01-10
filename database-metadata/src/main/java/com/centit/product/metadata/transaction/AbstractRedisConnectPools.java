package com.centit.product.metadata.transaction;

import com.centit.product.adapter.api.ISourceInfo;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhf
 */
public abstract class AbstractRedisConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRedisConnectPools.class);
    private static final
    Map<ISourceInfo, RedisClient> REDIS_CLIENT_POOLS = new ConcurrentHashMap<>();

    private AbstractRedisConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static RedisClient createRedisClient(ISourceInfo dsDesc)  {
        RedisURI uri =  RedisURI.create(dsDesc.getDatabaseUrl());
        uri.setUsername(dsDesc.getUsername());
        uri.setPassword(dsDesc.getClearPassword());
        /**
          RedisURI uri = RedisURI.builder().withHost(dsDesc.getDatabaseUrl())
            .withAuthentication(dsDesc.getUsername(), dsDesc.getClearPassword()).build();*/
        return RedisClient.create(uri);
    }

    static synchronized StatefulRedisConnection<String, String> getRedisConnect(ISourceInfo dsDesc) {
        RedisClient redisClient = REDIS_CLIENT_POOLS.get(dsDesc);
        if (redisClient == null) {
            redisClient = createRedisClient(dsDesc);
            REDIS_CLIENT_POOLS.put(dsDesc, redisClient);
        }
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        return connection;
    }

    static void closeConnect(StatefulRedisConnection<String, String> connection) {
        if(connection!=null){
            connection.close();
        }
    }
}
