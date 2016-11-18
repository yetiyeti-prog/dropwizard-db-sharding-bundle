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

package io.dropwizard.sharding.dao.locktest;

import io.dropwizard.sharding.sharding.LookupKey;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

/**
 * Some lookup object
 */
@Entity
@Table(name = "some_table", uniqueConstraints = {
        @UniqueConstraint(columnNames = "my_id")
})
@Data
@NoArgsConstructor
public class SomeLookupObject {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @LookupKey
    @Column(name = "my_id", unique = true)
    private String myId;

    @Column
    private String name;

    @Builder
    public SomeLookupObject(String myId, String name) {
        this.myId = myId;
        this.name = name;
    }
}
