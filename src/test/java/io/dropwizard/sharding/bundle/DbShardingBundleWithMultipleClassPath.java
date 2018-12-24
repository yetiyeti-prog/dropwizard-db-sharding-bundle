package io.dropwizard.sharding.bundle;

import com.google.common.collect.ImmutableList;
import io.dropwizard.sharding.DBShardingBundle;
import io.dropwizard.sharding.config.ShardedHibernateFactory;
import io.dropwizard.sharding.dao.LookupDao;
import io.dropwizard.sharding.dao.testdata.entities.TestEntity;
import io.dropwizard.sharding.dao.testdata.multi.MultiPackageTestEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class DbShardingBundleWithMultipleClassPath extends DBShardingBundleBaseTest  {



    @Override
    protected DBShardingBundle<TestConfig> getBundle() {
        return new DBShardingBundle<DBShardingBundleBaseTest.TestConfig>("io.dropwizard.sharding.dao.testdata.entities","io.dropwizard.sharding.dao.testdata.multi") {
            @Override
            protected ShardedHibernateFactory getConfig(DBShardingBundleBaseTest.TestConfig config) {
                return testConfig.getShards();
            }
        };
    }


    @Test
    public void testMultiPackage() throws Exception {

        DBShardingBundle<TestConfig> bundle = getBundle();

        bundle.initialize(bootstrap);
        bundle.initBundles(bootstrap);
        bundle.runBundles(testConfig, environment);
        bundle.run(testConfig, environment);
        LookupDao<MultiPackageTestEntity> lookupDao = DBShardingBundle.createParentObjectDao(bundle,MultiPackageTestEntity.class);

        MultiPackageTestEntity multiPackageTestEntity = MultiPackageTestEntity.builder()
                .text("Testing multi package scanning")
                .lookup("123")
                .build();

        Optional<MultiPackageTestEntity> saveMultiPackageTestEntity = lookupDao.save(multiPackageTestEntity);
        Assert.assertEquals(multiPackageTestEntity.getText(), saveMultiPackageTestEntity.get().getText());

        Optional<MultiPackageTestEntity> fetchedMultiPackageTestEntity = lookupDao.get(multiPackageTestEntity.getLookup());
        Assert.assertEquals(saveMultiPackageTestEntity.get().getText(), fetchedMultiPackageTestEntity.get().getText());

        LookupDao<TestEntity> testEntityLookupDao = DBShardingBundle.createParentObjectDao(bundle,TestEntity.class);

        TestEntity testEntity = TestEntity.builder()
                .externalId("E123")
                .text("Test Second Package")
                .build();
        Optional<TestEntity> savedTestEntity = testEntityLookupDao.save(testEntity);
        Assert.assertEquals(testEntity.getText(), savedTestEntity.get().getText());

        Optional<TestEntity> fetchedTestEntity = testEntityLookupDao.get(testEntity.getExternalId());
        Assert.assertEquals(savedTestEntity.get().getText(), fetchedTestEntity.get().getText());


    }
}
