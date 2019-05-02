package io.dropwizard.sharding.dao.testdata.multi;

import io.dropwizard.sharding.dao.testdata.entities.Transaction;
import io.dropwizard.sharding.sharding.LookupKey;
import lombok.*;

import javax.persistence.*;

@Entity
@Table(name="audits")
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class MultiPackageTestEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private long id;

    @LookupKey
    @Column(name = "lookup")
    private String lookup;

    @Column(name = "text")
    private String text;

}

