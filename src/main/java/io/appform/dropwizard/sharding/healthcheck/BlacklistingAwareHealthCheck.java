package io.appform.dropwizard.sharding.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BlacklistingAwareHealthCheck extends HealthCheck {

    private final int shardId;
    private final HealthCheck baseHealthCheck;
    private final ShardManager shardManager;

    public BlacklistingAwareHealthCheck(int shardId, HealthCheck baseHealthCheck, ShardManager shardManager) {
        this.shardId = shardId;
        this.baseHealthCheck = baseHealthCheck;
        this.shardManager = shardManager;
    }

    @Override
    protected Result check() {
        if (shardManager.isBlacklisted(shardId)) {
            log.info("returning healthy since shard is blacklisted [{}]", shardId);
            return Result.healthy();
        }
        Result result = baseHealthCheck.execute();
        log.debug(String.format("DBSharding HealthCheck for shardId: %s, Status: %s, %s", shardId,
                result.isHealthy(), result.getMessage()));
        return result;
    }
}
