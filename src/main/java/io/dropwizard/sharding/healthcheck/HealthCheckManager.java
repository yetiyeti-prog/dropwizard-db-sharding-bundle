package io.dropwizard.sharding.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistryListener;
import io.dropwizard.hibernate.SessionFactoryHealthCheck;
import io.dropwizard.setup.Environment;
import io.dropwizard.sharding.ShardInfoProvider;
import io.dropwizard.sharding.sharding.ShardManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class HealthCheckManager implements HealthCheckRegistryListener {

    private final String namespace;
    private final ShardManager shardManager;
    private final Map<String, ShardHealthCheckMeta> dbHealthChecks = new HashMap<>();
    private final Map<String, ShardHealthCheckMeta> wrappedHealthChecks = new HashMap<>();
    private final ShardInfoProvider shardInfoProvider;

    public HealthCheckManager(String namespace,
                              ShardManager shardManager,
                              ShardInfoProvider shardInfoProvider) {
        this.namespace = namespace;
        this.shardManager = shardManager;
        this.shardInfoProvider = shardInfoProvider;
    }


    @Override
    public void onHealthCheckAdded(String name, HealthCheck healthCheck) {
        if (!(healthCheck instanceof SessionFactoryHealthCheck)) {
            return;
        }

        String dbNamespace = shardInfoProvider.namespace(name);
        if (!Objects.equals(dbNamespace, this.namespace)) {
            return;
        }

        int shardId = shardInfoProvider.shardId(name);
        if (shardId == -1) {
            return;
        }

        log.info("db health check added {}", shardId);
        dbHealthChecks.put(name, ShardHealthCheckMeta.builder()
                .healthCheck(healthCheck)
                .shardId(shardId)
                .build());
    }

    @Override
    public void onHealthCheckRemoved(String s, HealthCheck healthCheck) {
        /*
        nothing needs to be done here
        */
    }

    public void manageHealthChecks(Environment environment) {
        dbHealthChecks.forEach((name, healthCheck) -> {
            environment.healthChecks().unregister(name);
            val hc = new BlacklistingAwareHealthCheck(healthCheck.getShardId(),
                    healthCheck.getHealthCheck(), shardManager);
            environment.healthChecks().register(name, hc);
            wrappedHealthChecks.put(name, ShardHealthCheckMeta.builder()
                    .healthCheck(hc)
                    .shardId(healthCheck.getShardId())
                    .build());
        });
    }


    public Map<Integer, Boolean> status() {
        return wrappedHealthChecks.values()
                .stream()
                .map(shardHealthCheckMeta -> new AbstractMap.SimpleEntry<>(shardHealthCheckMeta.getShardId(),
                        shardHealthCheckMeta.getHealthCheck().execute().isHealthy()))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }
}
