package kmakowski.kvs.redis

import io.github.kmakowski.json.Json
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.async.RedisAsyncCommands
import kmakowski.kvs.KeyValue
import kmakowski.kvs.KeyValueStore
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import java.time.Duration


class RedisKvsException (msg: String, throwable: Throwable): RuntimeException(msg, throwable)

private val log = LoggerFactory.getLogger("RedisKvs")

fun createRedisClient(redisUrl: String): RedisAsyncCommands<String, String> {
    log.info("Initializing redis connection")
    var redisURI = RedisURI.create(redisUrl)

    if (redisURI.isSsl) {
        redisURI.isVerifyPeer = false
    }

    log.info("will use redis uri: $redisURI")
    return RedisClient.create(redisURI).connect().async()
}

class RedisKvs(private val redis: RedisHolder, private val ttl: Duration = Duration.ofHours(1)): KeyValueStore {
    private val log = LoggerFactory.getLogger(RedisKvs::class.java)
    
    override suspend fun putObjectAsJson(key: String, any: Any) {
        log.debug("putObjectAsJson: key={}, any={}", key, any)
        val json = Json.serialize(any)

        try {
            putStringValue(key, json)
        } catch (e: Exception) {
            throw RedisKvsException("Failed to put object as JSON for key $key value $json", e)
        }
    }

    override suspend fun putStringValue(key: String, value: String) {
        log.debug("Will write to $key object $value")
        try {
            cmds().setex(key, ttl.toSeconds(), value).await()
        } catch (e: Exception) {
            throw RedisKvsException("Failed set $key", e)
        }
    }

    private fun cmds(): RedisAsyncCommands<String, String> = redis.get()

    override suspend fun <T> getObject(key: String, clazz: Class<T>): T? {
        val objectAsString = getObjectAsString(key)
        return if (objectAsString != null)
            Json.deserialize(objectAsString, clazz) 
        else null
    }

    override suspend fun getObjectAsString(key: String): String? {
        log.debug("getObjectAsString: key={}", key)
        try {
            return cmds().get(key).await()
        } catch (e: Exception) {
            throw RedisKvsException("Failed get $key", e)
        }
    }

    override suspend fun listObjects(
        listPrefix: String,
        startAfterKey: String?,
        maxItems: Int,
    ): List<KeyValue> {
        TODO("Not yet implemented")
    }

    override suspend fun addListItem(key: String, listName: String, item: String) {
        log.debug("addListItem: key={}, listName={}, item={}", key, listName, item)
        val prefix = listPrefix(key, listName)
        try {
            cmds().sadd(prefix, item).await()
        } catch (e: Exception) {
            throw RedisKvsException("Failed sadd $prefix", e)
        }
    }

    override suspend fun deleteListItem(key: String, listName: String, item: String) {
        log.debug("deleteListItem: key={}, listName={}, item={}", key, listName, item)
        val prefix = listPrefix(key, listName)

        try {
            cmds().srem(prefix, item).await()
        } catch (e: Exception) {
            throw RedisKvsException("Failed srem $prefix", e)
        }
    }

    override suspend fun getListItems(key: String, listName: String): List<String> {
        log.debug("getListItems: key={}, listName={}", key, listName)
        val prefix = listPrefix(key, listName)

        try {
            return cmds().smembers(prefix).await().toList()
        } catch (e: Exception) {
            throw RedisKvsException("Failed smembers $prefix", e)
        }
    }

    private fun listPrefix(key: String, listName: String) = "$key/$listName"

    override suspend fun listKeys(listPrefix: String): List<String> {
        TODO("Not yet implemented")
    }

    override suspend fun deleteObject(key: String) {
        try {
            cmds().del(key).await()
        } catch (e: Exception) {
            throw RedisKvsException("Failed del $key", e)
        }
    }
}