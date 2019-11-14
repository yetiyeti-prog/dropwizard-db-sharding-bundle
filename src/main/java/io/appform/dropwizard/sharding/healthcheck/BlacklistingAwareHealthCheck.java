
package io.appform.dropwizard.sharding.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import io.appform.dropwizard.sharding.sharding.ShardBlacklistingStore;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BlacklistingAwareHealthCheck extends HealthCheck {

    private final int shardId;
    private final HealthCheck baseHealthCheck;
    private final ShardManager shardManager;
    private final ShardBlacklistingStore blacklistingStore;

    public BlacklistingAwareHealthCheck(final int shardId,
                                        final HealthCheck baseHealthCheck,
                                        final ShardManager shardManager,
                                        final ShardBlacklistingStore blacklistingStore) {
        this.shardId = shardId;
        this.baseHealthCheck = baseHealthCheck;
        this.shardManager = shardManager;
        this.blacklistingStore = blacklistingStore;
    }

    @Override
    protected Result check() {
        if (shardManager.isBlacklisted(shardId)) {
            log.info("returning healthy since shard is blacklisted [{}]", shardId);
            return Result.healthy();
        }

        if (blacklistingStore.disableNativeHealthCheck()){
            return Result.healthy();
        }

        final Result result = baseHealthCheck.execute();
        log.debug("DBSharding HealthCheck for shardId: {}, Status: {}, {}", shardId,
                result.isHealthy(), result.getMessage());
        return result;
    }
}