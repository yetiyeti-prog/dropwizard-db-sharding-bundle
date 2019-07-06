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

package io.appform.dropwizard.sharding.sharding;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LegacyShardManagerTest {

   @Test(expected = IllegalArgumentException.class)
    public void testShardForBucket() throws Exception {
        LegacyShardManager shardManager = new LegacyShardManager(5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardForOddBucket() throws Exception {
        LegacyShardManager shardManager = new LegacyShardManager(9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShardForEvenNon2PowerBucket() throws Exception {
        LegacyShardManager shardManager = new LegacyShardManager(40);
    }

    @Test
    public void testShardFor64Bucket() throws Exception {
        LegacyShardManager shardManager = new LegacyShardManager(64);
        System.out.println(shardManager);
        assertEquals(63, shardManager.shardForBucket(999));
    }

    @Test
    public void testShardFor32Bucket() throws Exception {
        LegacyShardManager shardManager = new LegacyShardManager(32);
        System.out.println(shardManager);
        assertEquals(31, shardManager.shardForBucket(999));
    }

    @Test
    public void testShardFor16Bucket() throws Exception {
        LegacyShardManager shardManager = new LegacyShardManager(16);
        System.out.println(shardManager);
        assertEquals(15, shardManager.shardForBucket(999));
    }
}