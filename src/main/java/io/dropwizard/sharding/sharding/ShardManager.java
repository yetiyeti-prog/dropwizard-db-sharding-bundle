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

package io.dropwizard.sharding.sharding;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Manages shard to bucket mapping.
 */
@ToString
@Slf4j
public class ShardManager {
    private static final int MIN_BUCKET = 0;
    public static final int MAX_BUCKET = 999;

    private RangeMap<Integer, Integer> buckets = TreeRangeMap.create();

    @Builder
    public ShardManager(int numBuckets) {
        int interval = MAX_BUCKET / numBuckets;
        int shardCounter = 0;
        boolean endReached = false;
        for(int start = MIN_BUCKET; !endReached; start += interval, shardCounter++) {
            int end = start + interval - 1;
            endReached = !((MAX_BUCKET - start)  > (2 *interval));
            end =  endReached ? end + MAX_BUCKET - end : end;
            buckets.put(Range.closed(start, end), shardCounter);
        }
        log.info("Buckets to shard allocation: {}", buckets);
    }

    public int shardForBucket(int bucketId) {
        Preconditions.checkArgument(bucketId >=MIN_BUCKET && bucketId <= MAX_BUCKET, "Bucket id can only be in the range of [1-1000] (inclusive)");
        val entry = buckets.getEntry(bucketId);
        if(null == entry) {
            throw new IllegalAccessError("Bucket not mapped to any shard");
        }
        return entry.getValue();
    }


}
