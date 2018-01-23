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

package io.dropwizard.sharding.dao;

import io.dropwizard.sharding.caching.RelationalCache;
import io.dropwizard.sharding.sharding.BucketIdExtractor;
import io.dropwizard.sharding.sharding.ShardManager;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;

import java.util.List;
import java.util.Optional;

/**
 * A read/write through cache enabled {@link RelationalDao}
 */
public class CacheableRelationalDao<T> extends RelationalDao<T> {

    private RelationalCache<T> cache;

    public CacheableRelationalDao(List<SessionFactory> sessionFactories, Class<T> entityClass,
                                  ShardManager shardManager, BucketIdExtractor<String> bucketIdExtractor,
                                  RelationalCache<T> cache) {
        super(sessionFactories, entityClass, shardManager, bucketIdExtractor);
        this.cache = cache;
    }

    public Optional<T> get(String parentKey, Object key) {
        if(cache.exists(parentKey, key)) {
            return Optional.ofNullable(cache.get(parentKey, key));
        }
        T entity = super.get(parentKey, key, t-> t);
        if(entity != null) {
            cache.put(parentKey, key, entity);
        }
        return Optional.ofNullable(entity);
    }

    public Optional<T> save(String parentKey, T entity) throws Exception {
        T savedEntity = super.save(parentKey, entity, t -> t);
        if(savedEntity != null) {
            final String key = getKeyField().get(entity).toString();
            cache.put(parentKey, key, entity);
        }
        return Optional.ofNullable(savedEntity);
    }

    public List<T> select(String parentKey, DetachedCriteria criteria) throws Exception {
        List<T> result = cache.select(parentKey);
        if(result == null) {
            result = super.select(parentKey, criteria, 0, 10);
        }
        if(result != null) {
            cache.put(parentKey, result);
        }
        return result;
    }

    public List<T> select(String parentKey, DetachedCriteria criteria, int first, int numResults) throws Exception {
        List<T> result = cache.select(parentKey, first, numResults);
        if(result == null) {
            result = super.select(parentKey, criteria, first, numResults);
        }
        if(result != null) {
            cache.put(parentKey, first, numResults, result);
        }
        return select(parentKey, criteria, first, numResults, t-> t);
    }

}
