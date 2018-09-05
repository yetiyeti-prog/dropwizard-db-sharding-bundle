package io.dropwizard.sharding.bundle;

import io.dropwizard.sharding.DBShardingBundle;
import io.dropwizard.sharding.config.ShardedHibernateFactory;
import io.dropwizard.sharding.dao.testdata.entities.Order;
import io.dropwizard.sharding.dao.testdata.entities.OrderItem;

public class DBShardingBundleWithNamespaceTest extends DBShardingBundleBaseTest  {

    @Override
    protected DBShardingBundle<DBShardingBundleBaseTest.TestConfig> getBundle() {
        return new DBShardingBundle<TestConfig>("namespace", Order.class, OrderItem.class) {
            @Override
            protected ShardedHibernateFactory getConfig(TestConfig config) {
                return testConfig.getShards();
            }
        };
    }
}
