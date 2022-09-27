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

package io.appform.dropwizard.sharding.dao;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.appform.dropwizard.sharding.sharding.LookupKey;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import io.appform.dropwizard.sharding.utils.ShardCalculator;
import io.appform.dropwizard.sharding.utils.TransactionHandler;
import io.appform.dropwizard.sharding.utils.Transactions;
import io.dropwizard.hibernate.AbstractDAO;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.hibernate.LockMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * A dao to manage lookup and top level elements in the system. Can save and retrieve an object (tree) from any shard.
 * <b>Note:</b>
 * - The element must have only one String key for lookup.
 * - The key needs to be annotated with {@link LookupKey}
 * The entity can be retrieved from any shard using the key.
 */
@Slf4j
public class LookupDao<T> implements ShardedDao<T> {

    /**
     * This DAO wil be used to perform the ops inside a shard
     */
    private final class LookupDaoPriv extends AbstractDAO<T> {

        private final SessionFactory sessionFactory;

        public LookupDaoPriv(SessionFactory sessionFactory) {
            super(sessionFactory);
            this.sessionFactory = sessionFactory;
        }

        /**
         * Get an element from the shard.
         *
         * @param lookupKey Id of the object
         * @return Extracted element or null if not found.
         */
        T get(String lookupKey) {
            return getLocked(lookupKey, LockMode.READ);
        }

        T getLockedForWrite(String lookupKey) {
            return getLocked(lookupKey, LockMode.UPGRADE_NOWAIT);
        }

        /**
         * Get an element from the shard.
         *
         * @param lookupKey Id of the object
         * @return Extracted element or null if not found.
         */
        T getLocked(String lookupKey, LockMode lockMode) {
            return uniqueResult(currentSession()
                                        .createCriteria(entityClass)
                                        .add(Restrictions.eq(keyField.getName(), lookupKey))
                                        .setLockMode(lockMode));
        }

        /**
         * Save the lookup element. Returns the augmented element id any generated fields are present.
         *
         * @param entity Object to save
         * @return Augmented entity
         */
        T save(T entity) {
            return persist(entity);
        }

        void update(T entity) {
            currentSession().evict(entity); //Detach .. otherwise update is a no-op
            currentSession().update(entity);
        }

        /**
         * Run a query inside this shard and return the matching list.
         *
         * @param criteria selection criteria to be applied.
         * @return List of elements or empty list if none found
         */
        List<T> select(DetachedCriteria criteria) {
            return list(criteria.getExecutableCriteria(currentSession()));
        }

        long count(DetachedCriteria criteria) {
            return (long) criteria.getExecutableCriteria(currentSession())
                    .setProjection(Projections.rowCount())
                    .uniqueResult();
        }

        /**
         * Delete an object
         */
        boolean delete(String id) {
            return Optional.ofNullable(getLocked(id, LockMode.UPGRADE_NOWAIT))
                    .map(object -> {
                        currentSession().delete(object);
                        return true;
                    })
                    .orElse(false);

        }

        public int update(final UpdateOperationMeta updateOperationMeta) {
            Query query = currentSession().createNamedQuery(updateOperationMeta.getQueryName());
            updateOperationMeta.getParams().forEach(query::setParameter);
            return query.executeUpdate();
        }
    }

    private List<LookupDaoPriv> daos;
    private final Class<T> entityClass;

    @Getter
    private final ShardCalculator<String> shardCalculator;
    private final Field keyField;
    private final boolean roSkipTransaction;

    /**
     * Creates a new sharded DAO. The number of managed shards and bucketing is controlled by the {@link ShardManager}.
     *
     * @param sessionFactories a session provider for each shard
     * @param shardCalculator  calculator for shards
     */
    public LookupDao(
            List<SessionFactory> sessionFactories,
            Class<T> entityClass,
            ShardCalculator<String> shardCalculator,
            boolean roSkipTransaction) {
        this.daos = sessionFactories.stream().map(LookupDaoPriv::new).collect(Collectors.toList());
        this.entityClass = entityClass;
        this.shardCalculator = shardCalculator;
        this.roSkipTransaction = roSkipTransaction;

        Field fields[] = FieldUtils.getFieldsWithAnnotation(entityClass, LookupKey.class);
        Preconditions.checkArgument(fields.length != 0, "At least one field needs to be sharding key");
        Preconditions.checkArgument(fields.length == 1, "Only one field can be sharding key");
        keyField = fields[0];
        if (!keyField.isAccessible()) {
            try {
                keyField.setAccessible(true);
            }
            catch (SecurityException e) {
                log.error("Error making key field accessible please use a public method and mark that as LookupKey", e);
                throw new IllegalArgumentException("Invalid class, DAO cannot be created.", e);
            }
        }
        Preconditions.checkArgument(ClassUtils.isAssignable(keyField.getType(), String.class),
                                    "Key field must be a string");
    }

    /**
     * Get an object on the basis of key (value of field annotated with {@link LookupKey}) from any shard.
     * <b>Note:</b> Lazy loading will not work once the object is returned.
     * If you need lazy loading functionality use the alternate {@link #get(String, Function)} method.
     *
     * @param key The value of the key field to look for.
     * @return The entity
     * @throws Exception if backing dao throws
     */
    public Optional<T> get(String key) throws Exception {
        return Optional.ofNullable(get(key, t -> t));
    }

    /**
     * Get an object on the basis of key (value of field annotated with {@link LookupKey}) from any shard
     * and applies the provided function/lambda to it. The return from the handler becomes the return to the get function.
     * <b>Note:</b> The transaction is open when handler is applied. So lazy loading will work inside the handler.
     * Once get returns, lazy loading will nt owrok.
     *
     * @param key     The value of the key field to look for.
     * @param handler Handler function/lambda that receives the retrieved object.
     * @return Whatever is returned by the handler function
     * @throws Exception if backing dao throws
     */
    public <U> U get(String key, Function<T, U> handler) throws Exception {
        int shardId = shardCalculator.shardId(key);
        LookupDaoPriv dao = daos.get(shardId);
        return Transactions.execute(dao.sessionFactory, true, dao::get, key, handler);
    }

    /**
     * Check if object with specified key exists in any shard.
     *
     * @param key id of the element to look for
     * @return true/false depending on if it's found or not.
     * @throws Exception if backing dao throws
     */
    public boolean exists(String key) throws Exception {
        return get(key).isPresent();
    }

    /**
     * Saves an entity on proper shard based on hash of the value in the key field in the object.
     * The updated entity is returned. If Cascade is specified, this can be used
     * to save an object tree based on the shard of the top entity that has the key field.
     * <b>Note:</b> Lazy loading will not work on the augmented entity. Use the alternate {@link #save(Object, Function)} for that.
     *
     * @param entity Entity to save
     * @return Entity
     * @throws Exception if backing dao throws
     */
    public Optional<T> save(T entity) throws Exception {
        return Optional.ofNullable(save(entity, t -> t));
    }

    /**
     * Save an object on the basis of key (value of field annotated with {@link LookupKey}) to target shard
     * and applies the provided function/lambda to it. The return from the handler becomes the return to the get function.
     * <b>Note:</b> Handler is executed in the same transactional context as the save operation.
     * So any updates made to the object in this context will also get persisted.
     *
     * @param entity  The value of the key field to look for.
     * @param handler Handler function/lambda that receives the retrieved object.
     * @return The entity
     * @throws Exception if backing dao throws
     */
    public <U> U save(T entity, Function<T, U> handler) throws Exception {
        final String key = keyField.get(entity).toString();
        int shardId = shardCalculator.shardId(key);
        log.debug("Saving entity of type {} with key {} to shard {}", entityClass.getSimpleName(), key, shardId);
        LookupDaoPriv dao = daos.get(shardId);
        return Transactions.execute(dao.sessionFactory, false, dao::save, entity, handler);
    }

    public boolean updateInLock(String id, Function<Optional<T>, T> updater) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return updateImpl(id, dao::getLockedForWrite, updater, dao);
    }

    public boolean update(String id, Function<Optional<T>, T> updater) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return updateImpl(id, dao::get, updater, dao);
    }

    public int updateUsingQuery(String id, UpdateOperationMeta updateOperationMeta) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return Transactions.execute(dao.sessionFactory, false, dao::update, updateOperationMeta);
    }

    private boolean updateImpl(
            String id,
            Function<String, T> getter,
            Function<Optional<T>, T> updater,
            LookupDaoPriv dao) {
        try {
            return Transactions.<T, String, Boolean>execute(dao.sessionFactory, true, getter, id, entity -> {
                T newEntity = updater.apply(Optional.ofNullable(entity));
                if (null == newEntity) {
                    return false;
                }
                dao.update(newEntity);
                return true;
            });
        }
        catch (Exception e) {
            throw new RuntimeException("Error updating entity: " + id, e);
        }
    }

    public LockedContext<T> lockAndGetExecutor(String id) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return new LockedContext<>(shardId, dao.sessionFactory, dao::getLockedForWrite, id);
    }

    public ReadOnlyContext<T> readOnlyExecutor(String id) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return new ReadOnlyContext<>(shardId, dao.sessionFactory, key -> dao.getLocked(key, LockMode.NONE), null, id, roSkipTransaction);
    }

    public ReadOnlyContext<T> readOnlyExecutor(String id, Supplier<Boolean> entityPopulator) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return new ReadOnlyContext<>(shardId,
                                     dao.sessionFactory,
                                     key -> dao.getLocked(key, LockMode.NONE),
                                     entityPopulator,
                                     id, roSkipTransaction);
    }

    public LockedContext<T> saveAndGetExecutor(T entity) {
        String id;
        try {
            id = keyField.get(entity).toString();
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return new LockedContext<>(shardId, dao.sessionFactory, dao::save, entity);
    }

    /**
     * Queries using the specified criteria across all shards and returns the result.
     * <b>Note:</b> This method runs the query serially and it's usage is not recommended.
     *
     * @param criteria The selct criteria
     * @return List of elements or empty if none match
     */
    public List<T> scatterGather(DetachedCriteria criteria) {
        return daos.stream().map(dao -> {
            try {
                return Transactions.execute(dao.sessionFactory, true, dao::select, criteria);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).flatMap(Collection::stream).collect(Collectors.toList());
    }

    /**
     * Queries using the specified criteria across all shards and returns the counts of rows satisfying the criteria.
     * <b>Note:</b> This method runs the query serially and it's usage is not recommended.
     *
     * @param criteria The select criteria
     * @return List of counts in each shard
     */
    public List<Long> count(DetachedCriteria criteria) {
        return daos.stream().map(dao -> {
            try {
                return Transactions.execute(dao.sessionFactory, true, dao::count, criteria);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Queries across various shards and returns the results.
     * <b>Note:</b> This method runs the query serially and is efficient over scatterGather and serial get of all key
     *
     * @param keys The list of lookup keys
     * @return List of elements or empty if none match
     */
    public List<T> get(List<String> keys) {
        Map<Integer, List<String>> lookupKeysGroupByShards = keys.stream()
                .collect(
                        Collectors.groupingBy(shardCalculator::shardId, Collectors.toList()));

        return lookupKeysGroupByShards.keySet().stream().map(shardId -> {
            try {
                DetachedCriteria criteria = DetachedCriteria.forClass(entityClass)
                        .add(Restrictions.in(keyField.getName(), lookupKeysGroupByShards.get(shardId)));
                return Transactions.execute(daos.get(shardId).sessionFactory,
                                            true,
                                            daos.get(shardId)::select,
                                            criteria);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public <U> U runInSession(String id, Function<Session, U> handler) {
        int shardId = shardCalculator.shardId(id);
        LookupDaoPriv dao = daos.get(shardId);
        return Transactions.execute(dao.sessionFactory, handler);
    }

    public boolean delete(String id) {
        int shardId = shardCalculator.shardId(id);
        return Transactions.execute(daos.get(shardId).sessionFactory, false, daos.get(shardId)::delete, id);
    }

    protected Field getKeyField() {
        return this.keyField;
    }

    @Getter
    public static class ReadOnlyContext<T> {
        private final int shardId;
        private final SessionFactory sessionFactory;
        private final Function<String, T> getter;
        private final Supplier<Boolean> entityPopulator;
        private final String key;
        private final List<Function<T, Void>> operations = Lists.newArrayList();
        private final boolean skipTransaction;

        public ReadOnlyContext(
                int shardId,
                SessionFactory sessionFactory,
                Function<String, T> getter,
                Supplier<Boolean> entityPopulator,
                String key,
                boolean skipTransaction) {
            this.shardId = shardId;
            this.sessionFactory = sessionFactory;
            this.getter = getter;
            this.entityPopulator = entityPopulator;
            this.key = key;
            this.skipTransaction = skipTransaction;
        }


        public ReadOnlyContext<T> apply(Function<T, Void> handler) {
            this.operations.add(handler);
            return this;
        }


        public <U> ReadOnlyContext<T> readOneAugmentParent(
                RelationalDao<U> relationalDao,
                DetachedCriteria criteria,
                BiConsumer<T, List<U>> consumer) {
            return readAugmentParent(relationalDao, criteria, 0, 1, consumer, p -> true);
        }

        public <U> ReadOnlyContext<T> readAugmentParent(
                RelationalDao<U> relationalDao,
                DetachedCriteria criteria,
                int first,
                int numResults,
                BiConsumer<T, List<U>> consumer) {
            return readAugmentParent(relationalDao, criteria, first, numResults, consumer, p -> true);
        }

        public <U> ReadOnlyContext<T> readOneAugmentParent(
                RelationalDao<U> relationalDao,
                DetachedCriteria criteria,
                BiConsumer<T, List<U>> consumer,
                Predicate<T> filter) {
            return readAugmentParent(relationalDao, criteria, 0, 1, consumer, filter);
        }

        public <U> ReadOnlyContext<T> readAugmentParent(
                RelationalDao<U> relationalDao,
                DetachedCriteria criteria,
                int first,
                int numResults,
                BiConsumer<T, List<U>> consumer,
                Predicate<T> filter) {
            return apply(parent -> {
                if (filter.test(parent)) {
                    try {
                        consumer.accept(parent, relationalDao.select(this, criteria, first, numResults));
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            });
        }

        public Optional<T> execute() {
            var result = executeImpl();
            if (null == result
                    && null != entityPopulator
                    && Boolean.TRUE.equals(entityPopulator.get())) {//Try to populate entity (maybe from cold store etc)
                result = executeImpl();
            }
            return Optional.ofNullable(result);
        }

        private T executeImpl() {
            TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, true, this.skipTransaction);
            transactionHandler.beforeStart();
            try {
                T result = getter.apply(key);
                if (null == result) {
                    return null;
                }
                operations.forEach(operation -> operation.apply(result));
                return result;
            }
            catch (Exception e) {
                transactionHandler.onError();
                throw e;
            }
            finally {
                transactionHandler.afterEnd();
            }
        }
    }

    /**
     * A context for a shard
     */
    @Getter
    public static class LockedContext<T> {
        @FunctionalInterface
        public interface Mutator<T> {
            void mutator(T parent);
        }

        enum Mode {READ, INSERT}

        private final int shardId;
        private final SessionFactory sessionFactory;
        private Function<String, T> function;
        private Function<T, T> saver;
        private T entity;
        private String key;
        private List<Function<T, Void>> operations = Lists.newArrayList();
        private final Mode mode;

        public LockedContext(int shardId,
                             SessionFactory sessionFactory,
                             Function<String, T> getter,
                             String key) {
            this.shardId = shardId;
            this.sessionFactory = sessionFactory;
            this.function = getter;
            this.key = key;
            this.mode = Mode.READ;
        }

        public LockedContext(int shardId, SessionFactory sessionFactory, Function<T, T> saver, T entity) {
            this.shardId = shardId;
            this.sessionFactory = sessionFactory;
            this.saver = saver;
            this.entity = entity;
            this.mode = Mode.INSERT;
        }

        public LockedContext<T> mutate(Mutator<T> mutator) {
            return apply(parent -> {
                mutator.mutator(parent);
                return null;
            });
        }

        public LockedContext<T> apply(Function<T, Void> handler) {
            this.operations.add(handler);
            return this;
        }

        public <U> LockedContext<T> save(RelationalDao<U> relationalDao,
                                         Function<T, U> entityGenerator) {
            return apply(parent -> {
                try {
                    U entity = entityGenerator.apply(parent);
                    relationalDao.save(this, entity);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public <U> LockedContext<T> saveAll(RelationalDao<U> relationalDao, Function<T, List<U>> entityGenerator) {
            return apply(parent -> {
                try {
                    List<U> entities = entityGenerator.apply(parent);
                    for (U entity : entities) {
                        relationalDao.save(this, entity);
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public <U> LockedContext<T> save(RelationalDao<U> relationalDao, U entity, Function<U, U> handler) {
            return apply(parent -> {
                try {
                    relationalDao.save(this, entity, handler);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public <U> LockedContext<T> updateUsingQuery(
                RelationalDao<U> relationalDao,
                UpdateOperationMeta updateOperationMeta) {
            return apply(parent -> {
                try {
                    relationalDao.updateUsingQuery(this, updateOperationMeta);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public <U> LockedContext<T> update(RelationalDao<U> relationalDao, Object id, Function<U, U> handler) {
            return apply(parent -> {
                try {
                    relationalDao.update(this, id, handler);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public <U> LockedContext<T> createOrUpdate(
                RelationalDao<U> relationalDao,
                DetachedCriteria criteria,
                Function<U, U> updater,
                Supplier<U> entityGenerator) {
            return apply(parent -> {
                try {
                    relationalDao.createOrUpdate(this, criteria, updater, entityGenerator);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public <U> LockedContext<T> update(
                RelationalDao<U> relationalDao,
                DetachedCriteria criteria,
                Function<U, U> updater,
                BooleanSupplier updateNext) {
            return apply(parent -> {
                try {
                    relationalDao.update(this, criteria, updater, updateNext);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }

        public LockedContext<T> filter(Predicate<T> predicate) {
            return filter(predicate, new IllegalArgumentException("Predicate check failed"));
        }

        public LockedContext<T> filter(Predicate<T> predicate, RuntimeException failureException) {
            return apply(parent -> {
                boolean result = predicate.test(parent);
                if (!result) {
                    throw failureException;
                }
                return null;
            });
        }

        public T execute() {
            TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, false);
            transactionHandler.beforeStart();
            try {
                T result = generateEntity();
                operations
                        .forEach(operation -> operation.apply(result));
                return result;
            }
            catch (Exception e) {
                transactionHandler.onError();
                throw e;
            }
            finally {
                transactionHandler.afterEnd();
            }
        }

        private T generateEntity() {
            T result = null;
            switch (mode) {
                case READ:
                    result = function.apply(key);
                    if (result == null) {
                        throw new RuntimeException("Entity doesn't exist for key: " + key);
                    }
                    break;
                case INSERT:
                    result = saver.apply(entity);
                    break;
                default:
                    break;

            }
            return result;
        }
    }
}
