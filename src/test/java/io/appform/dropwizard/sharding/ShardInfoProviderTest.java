package io.appform.dropwizard.sharding;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kanika.khetawat on 14/10/19
 */
public class ShardInfoProviderTest {

    @Test
    public void testGetShardId() {
        ShardInfoProvider shardInfoProvider = new ShardInfoProvider("default");
        int shardId = shardInfoProvider.shardId("connectionpool-default-2");
        Assert.assertEquals(2, shardId);

        shardId = shardInfoProvider.shardId("connectionpool");
        Assert.assertEquals(-1, shardId);

        shardId = shardInfoProvider.shardId("connectionpool-test-1");
        Assert.assertEquals(1, shardId);

        shardId = shardInfoProvider.shardId("connectionpool-1");
        Assert.assertEquals(-1, shardId);

        shardId = shardInfoProvider.shardId("default-1");
        Assert.assertEquals(-1, shardId);
    }

    @Test
    public void testGetNamespace() {
        ShardInfoProvider shardInfoProvider = new ShardInfoProvider("default");
        String namespace = shardInfoProvider.namespace("connectionpool-default-2");
        Assert.assertEquals("default", namespace);

        namespace = shardInfoProvider.namespace("connectionpool");
        Assert.assertNull(namespace);

        namespace = shardInfoProvider.namespace("connectionpool-default");
        Assert.assertNull(namespace);

        namespace = shardInfoProvider.namespace("connectionpool-test-1");
        Assert.assertEquals("test", namespace);
    }

    @Test
    public void testGetShardName() {
        ShardInfoProvider shardInfoProvider = new ShardInfoProvider("default");
        String shardName = shardInfoProvider.shardName(1);
        Assert.assertEquals("connectionpool-default-1", shardName);
    }
}
