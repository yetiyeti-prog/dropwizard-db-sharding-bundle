package io.dropwizard.sharding.bundle;

import io.dropwizard.sharding.DBShardingBundle;
import io.dropwizard.sharding.config.ShardedHibernateFactory;
import io.dropwizard.sharding.dao.testdata.entities.Order;
import io.dropwizard.sharding.dao.testdata.entities.OrderItem;

/**
 * Created by tushar.mandar on 4/25/17.
 */
public class DBShardingBundleWithEntityTest extends DBShardingBundleBaseTest {

    @Override
    protected DBShardingBundle<DBShardingBundleBaseTest.TestConfig> getBundle() {
        return new DBShardingBundle<TestConfig>(true, Order.class, OrderItem.class) {
            @Override
            protected ShardedHibernateFactory getConfig(TestConfig config) {
                return testConfig.getShards();
            }
        };
    }
}
