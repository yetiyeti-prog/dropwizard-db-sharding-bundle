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

package io.dropwizard.sharding.utils;

import org.hibernate.SessionFactory;

import java.util.Optional;
import java.util.function.Function;

/**
 * Utility functional class for running transactions.
 */
public class Transactions {
    private Transactions() {}

    public static <T, U> Optional<T> executeAndResolve(SessionFactory sessionFactory, Function<U, T> function, U arg) throws Exception {
        return executeAndResolve(sessionFactory, false, function, arg);
    }

    public static <T, U> Optional<T> executeAndResolve(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg) throws Exception {
        T result = execute(sessionFactory, readOnly, function, arg);
        if(null == result) {
            return Optional.empty();
        }
        return Optional.of(result);
    }

    public static <T, U> T execute(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg) throws Exception {
        return execute(sessionFactory, readOnly, function, arg, t -> t);
    }

    public static <T, U, V> V execute(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg, Function<T, V> handler) throws Exception {
        TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, readOnly);
        transactionHandler.beforeStart();
        try {
            T result = function.apply(arg);
            V returnValue = handler.apply(result);
            transactionHandler.afterEnd();
            return returnValue;
        } catch (Exception e) {
            transactionHandler.onError();
            throw e;
        }
    }
}
