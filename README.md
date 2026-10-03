# kvs-redis

Redis implementation of the [`kvs`](https://github.com/kmakowski/kvs) `KeyValueStore` interface, plus a Redis streams queue.

```xml
<dependency>
    <groupId>io.github.kmakowski</groupId>
    <artifactId>kvs-redis</artifactId>
    <version>1.0.0</version>
</dependency>
```

Publish a release to Maven Central with a GPG key and a [Central Portal](https://central.sonatype.com) user token in `~/.m2/settings.xml` under server id `central`:

```bash
mvn deploy -Pcentral
```
