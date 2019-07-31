/*
 * Copyright 2019 Santanu Sinha <santanu.sinha@gmail.com>
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

package io.appform.dropwizard.sharding.sharding;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BalancedShardManagerTest {

   @Test(expected = IllegalArgumentException.class)
    public void testShardForBucket() throws Exception {
        new BalancedShardManager(5);
    }

   @Test(expected = IllegalArgumentException.class)
    public void testShardForOddBucket() throws Exception {
        new BalancedShardManager(9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardForEvenNon2PowerBucket() throws Exception {
        new BalancedShardManager(40);
    }

    @Test
    public void testShardFor64Bucket() throws Exception {
        BalancedShardManager shardManager = new BalancedShardManager(64);
        assertEquals(63, shardManager.shardForBucket(1023));
    }

    @Test
    public void testShardFor32Bucket() throws Exception {
        BalancedShardManager shardManager = new BalancedShardManager(32);
        assertEquals(31, shardManager.shardForBucket(1023));
    }

    @Test
    public void testShardFor31Bucket() throws Exception {
        BalancedShardManager shardManager = new BalancedShardManager(16);
        assertEquals(15, shardManager.shardForBucket(1023));
    }
}