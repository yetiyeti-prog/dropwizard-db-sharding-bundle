# Dropwizard DB Sharding Bundle

Application level sharding for traditional relational databases.
Apache Licensed

## Sharding logic
![Sharding logic depiction](resources/ApplicationLevelSharding.png)
## Principle: Evenly distribute data irrespective of actual number of physical shards chosen by application owners.
Strategy:
 * Pick a hashing algorithm with good distribution characteristics for most inputs.
 * Pick a large bucket size. Sharding key will be hashed to one of these buckets as a first step.
 * Using a large bucket size is expected to distribute keys fairly evenly. Buckets are grouped together to form intervals with each interval mapping to a physical shard.
 This bucket to shard mapping is created at application startup time.

### Shard to bucket mapping
 * For purpose of evenly distributing data, a hashing algorithm is used to hash a key to one of a large number of buckets.
Current bucket size is **1024**.

* For Bucket count K, and Physical shards N, usually K >> N. Buckets are divided into intervals of size N/K and each interval is mapped one-to-one to a physical shard.

### Hashing algorithm for uniform sharding
**Hashing.murmur3_128()** from Guava library is used which yields a 128 bit value corresponding to the hashing key.
This value is converted to integer to get bucket and in turn physical shard to which value for the key will be saved and retrieved.

### What happens if an application owner decides to change the number of physical shards. For example from 16 to 32.
Resharding will be required to persist data to its new shard.

## DAOs
Daos are supposed to be objects to interact with datasource.
Dao classes in db sharding bundle are a layer above Hibernate. Daos interact with database via Hibernate session.

## Types of DAOs supported

### RelationalDao
 * A dao used to work with entities related to a parent shard. The parent may or maynot be physically present.
 * A murmur 128 hash of the string parent key is used to route the save and retrieve calls from the proper shard.

####
##What is the concept of parent key in RelationalDao?
One of the main requirements of sharding relational data is to colocate data for entities that might be
accessed or retrived together. For example, a merchant's profile, her store information, her payment options might be pulled together
in some flows, so it should be located on the same shard for a merchant M.
For this purpose, shard containing merchant M's primary profile info can be considered as parent shard and merchant's Id can be treated as parent key.
This parent key can be used for persisting related entities for the merchant using Relational Dao.


##Is it necessary for different entities to use same parent key?
Only for related entities, it makes sense to pick and use same parent key.

##Is sharding key specific to table or entire db?
Sharding key is required to shard an entity's data among different shards, it may have no relation in general
with any other entity. But, mostly entities are related to each other, so picking one sharding key/parent key
to persist a number of related entities helps to keep code predictable and maintainable.

##What is alternative for persisting entity that is a root entity in itself and has no clear parent?
LookupDao can be used for this purpose. Unlike RelationalDao, LookupDao has the concept of LookupKey.
One of the fields in the entity can be annotated with @LookupKey annotation to use this field for saving and retrieving
entity. This field will be treated as hashing key with the Dao.

## Shard blacklisting
TBD


##Features
* Pagination support

## Usage


The project dependencies are:
```
<dependency>
    <groupId>io.appform.dropwizard.sharding</groupId>
    <artifactId>db-sharding-bundle</artifactId>
    <version>1.3.13-4</version>
</dependency>
```
# NOTE
- Package and group id has changed from `io.dropwizard.sharding` to `io.appfrom.dropwizard.sharding` from 1.3.12-3.
- static create* methods have been replaced with instance methods from 1.3.13-4
