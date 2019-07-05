/*
 * Copyright 2016 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.dropwizard.sharding;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.AbstractDAO;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.SessionFactoryFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.sharding.admin.BlacklistShardTask;
import io.dropwizard.sharding.admin.UnblacklistShardTask;
import io.dropwizard.sharding.caching.LookupCache;
import io.dropwizard.sharding.caching.RelationalCache;
import io.dropwizard.sharding.config.ShardedHibernateFactory;
import io.dropwizard.sharding.dao.*;
import io.dropwizard.sharding.healthcheck.HealthCheckManager;
import io.dropwizard.sharding.sharding.BucketIdExtractor;
import io.dropwizard.sharding.sharding.ShardBlacklistingStore;
import io.dropwizard.sharding.sharding.ShardManager;
import io.dropwizard.sharding.sharding.impl.ConsistentHashBucketIdExtractor;
import io.dropwizard.sharding.utils.ShardCalculator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.hibernate.SessionFactory;
import org.reflections.Reflections;

import javax.persistence.Entity;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base for bundles. This cannot be used by clients. Use one of the derived classes.
 */
@Slf4j
abstract class DBShardingBundleBase<T extends Configuration> implements ConfiguredBundle<T> {

    private static final String DEFAULT_NAMESPACE = "default";
    private static final String SHARD_ENV = "db.shards";
    private static final String DEFAULT_SHARDS = "2";

    private List<HibernateBundle<T>> shardBundles = Lists.newArrayList();
    @Getter
    private List<SessionFactory> sessionFactories;
    @Getter
    private ShardManager shardManager;
    @Getter
    private String dbNamespace;
    @Getter
    private int numShards;

    private ShardInfoProvider shardInfoProvider;

    private HealthCheckManager healthCheckManager;

    protected DBShardingBundleBase(
            String dbNamespace,
            Class<?> entity,
            Class<?>... entities) {
        this.dbNamespace = dbNamespace;
        this.shardInfoProvider = new ShardInfoProvider(dbNamespace);
        this.healthCheckManager = new HealthCheckManager(dbNamespace, shardManager, shardInfoProvider);
        val inEntities = ImmutableList.<Class<?>>builder().add(entity).add(entities).build();
        init(inEntities);
    }

    protected DBShardingBundleBase(String dbNamespace, List<String> classPathPrefixList) {
        this.dbNamespace = dbNamespace;
        Set<Class<?>> entities = new Reflections(classPathPrefixList).getTypesAnnotatedWith(Entity.class);
        Preconditions.checkArgument(!entities.isEmpty(), String.format("No entity class found at %s", String.join(",", classPathPrefixList)));
        val inEntities = ImmutableList.<Class<?>>builder().addAll(entities).build();
        init(inEntities);
    }

    protected DBShardingBundleBase(Class<?> entity, Class<?>... entities) {
        this(DEFAULT_NAMESPACE, entity, entities);
    }

    protected DBShardingBundleBase(String... classPathPrefixes) {
        this(DEFAULT_NAMESPACE, Arrays.asList(classPathPrefixes));
    }

    protected abstract ShardManager createShardManager(int numShards);

    protected abstract ShardManager createShardManager(int numShards, ShardBlacklistingStore blacklistingStore);

    private void init(final ImmutableList<Class<?>> inEntities) {
        String numShardsEnv = System.getProperty(String.join(".", dbNamespace, DEFAULT_NAMESPACE),
                System.getProperty(SHARD_ENV, DEFAULT_SHARDS));

        this.numShards = Integer.parseInt(numShardsEnv);
        val blacklistingStore = getBlacklistingStore();
        this.shardManager = blacklistingStore != null
                ? createShardManager(numShards, blacklistingStore)
                : createShardManager(numShards);
        this.shardInfoProvider = new ShardInfoProvider(dbNamespace);
        this.healthCheckManager = new HealthCheckManager(dbNamespace, shardManager, shardInfoProvider);
        for (int i = 0; i < numShards; i++) {
            final int finalI = i;
            shardBundles.add(new HibernateBundle<T>(inEntities, new SessionFactoryFactory()) {
                @Override
                protected String name() {
                    return shardInfoProvider.shardName(finalI);
                }

                @Override
                public PooledDataSourceFactory getDataSourceFactory(T t) {
                    return getConfig(t).getShards().get(finalI);
                }
            });
        }
    }

    @Override
    public void run(T configuration, Environment environment) {
        sessionFactories = shardBundles.stream().map(HibernateBundle::getSessionFactory).collect(Collectors.toList());
        environment.admin().addTask(new BlacklistShardTask(shardManager));
        environment.admin().addTask(new UnblacklistShardTask(shardManager));
        healthCheckManager.manageHealthChecks(environment);
    }


    @Override
    @SuppressWarnings("unchecked")
    public void initialize(Bootstrap<?> bootstrap) {
        bootstrap.getHealthCheckRegistry().addListener(healthCheckManager);
        shardBundles.forEach(hibernateBundle -> bootstrap.addBundle((ConfiguredBundle) hibernateBundle));
    }

    @VisibleForTesting
    public void runBundles(T configuration, Environment environment) {
        shardBundles.forEach(hibernateBundle -> {
            try {
                hibernateBundle.run(configuration, environment);
            } catch (Exception e) {
                log.error("Error initializing db sharding bundle", e);
                throw new RuntimeException(e);
            }
        });
    }

    @VisibleForTesting
    public void initBundles(Bootstrap bootstrap) {
        shardBundles.forEach(hibernameBundle -> initialize(bootstrap));
    }

    @VisibleForTesting
    public Map<Integer, Boolean> healthStatus() {
        return healthCheckManager.status();
    }

    protected abstract ShardedHibernateFactory getConfig(T config);

    protected ShardBlacklistingStore getBlacklistingStore() {
        return null;
    }

    public static <EntityType, T extends Configuration>
    LookupDao<EntityType> createParentObjectDao(DBShardingBundleBase<T> bundle, Class<EntityType> clazz) {
        return new LookupDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, new ConsistentHashBucketIdExtractor<>(bundle.shardManager)));
    }

    public static <EntityType, T extends Configuration>
    CacheableLookupDao<EntityType> createParentObjectDao(DBShardingBundleBase<T> bundle, Class<EntityType> clazz, LookupCache<EntityType> cacheManager) {
        return new CacheableLookupDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, new ConsistentHashBucketIdExtractor<>(bundle.shardManager)), cacheManager);
    }

    public static <EntityType, T extends Configuration>
    LookupDao<EntityType> createParentObjectDao(DBShardingBundleBase<T> bundle,
                                                Class<EntityType> clazz,
                                                BucketIdExtractor<String> bucketIdExtractor) {
        return new LookupDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, bucketIdExtractor));
    }

    public static <EntityType, T extends Configuration>
    CacheableLookupDao<EntityType> createParentObjectDao(DBShardingBundleBase<T> bundle, Class<EntityType> clazz, BucketIdExtractor<String> bucketIdExtractor, LookupCache<EntityType> cacheManager) {
        return new CacheableLookupDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, bucketIdExtractor), cacheManager);
    }


    public static <EntityType, T extends Configuration>
    RelationalDao<EntityType> createRelatedObjectDao(DBShardingBundleBase<T> bundle, Class<EntityType> clazz) {
        return new RelationalDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, new ConsistentHashBucketIdExtractor<>(bundle.shardManager)));
    }


    public static <EntityType, T extends Configuration>
    CacheableRelationalDao<EntityType> createRelatedObjectDao(DBShardingBundleBase<T> bundle, Class<EntityType> clazz, RelationalCache<EntityType> cacheManager) {
        return new CacheableRelationalDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, new ConsistentHashBucketIdExtractor<>(bundle.shardManager)), cacheManager);
    }


    public static <EntityType, T extends Configuration>
    RelationalDao<EntityType> createRelatedObjectDao(DBShardingBundleBase<T> bundle,
                                                     Class<EntityType> clazz,
                                                     BucketIdExtractor<String> bucketIdExtractor) {
        return new RelationalDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, bucketIdExtractor));
    }

    public static <EntityType, T extends Configuration>
    CacheableRelationalDao<EntityType> createRelatedObjectDao(DBShardingBundleBase<T> bundle, Class<EntityType> clazz, BucketIdExtractor<String> bucketIdExtractor, RelationalCache<EntityType> cacheManager) {
        return new CacheableRelationalDao<>(bundle.sessionFactories, clazz, new ShardCalculator<>(bundle.shardManager, bucketIdExtractor), cacheManager);
    }


    public static <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
    WrapperDao<EntityType, DaoType> createWrapperDao(DBShardingBundleBase<T> bundle, Class<DaoType> daoTypeClass) {
        return new WrapperDao<>(bundle.sessionFactories, daoTypeClass, new ShardCalculator<>(bundle.shardManager, new ConsistentHashBucketIdExtractor<>(bundle.shardManager)));
    }

    public static <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
    WrapperDao<EntityType, DaoType> createWrapperDao(DBShardingBundleBase<T> bundle,
                                                     Class<DaoType> daoTypeClass,
                                                     BucketIdExtractor<String> bucketIdExtractor) {
        return new WrapperDao<>(bundle.sessionFactories, daoTypeClass, new ShardCalculator<>(bundle.shardManager, bucketIdExtractor));
    }

    public static <EntityType, DaoType extends AbstractDAO<EntityType>, T extends Configuration>
    WrapperDao<EntityType, DaoType> createWrapperDao(DBShardingBundleBase<T> bundle, Class<DaoType> daoTypeClass,
                                                     Class[] extraConstructorParamClasses, Class[] extraConstructorParamObjects) {
        return new WrapperDao<>(bundle.sessionFactories, daoTypeClass,
                extraConstructorParamClasses, extraConstructorParamObjects,
                new ShardCalculator<>(bundle.shardManager, new ConsistentHashBucketIdExtractor<>(bundle.shardManager)));
    }
}
