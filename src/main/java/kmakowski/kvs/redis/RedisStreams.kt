package kmakowski.kvs.redis

import io.github.kmakowski.json.Json
import io.lettuce.core.Consumer
import io.lettuce.core.XGroupCreateArgs
import io.lettuce.core.XReadArgs
import io.lettuce.core.api.async.RedisAsyncCommands
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory

class RedisHolder(private val redisUrl: String) {
    private val redisClient: RedisAsyncCommands<String, String> by lazy {
        createRedisClient(redisUrl)
    }

    fun get(): RedisAsyncCommands<String, String> {
        return redisClient
    }
}

class RedisStreamQueue(private val redis: RedisHolder) {
    private val log = LoggerFactory.getLogger(RedisStreamQueue::class.java)

    suspend fun publish(stream: String, message: Any): String {
        val json = Json.serialize(message)
        return redis.get().xadd(stream, mapOf("data" to json)).await()
    }

    suspend fun createConsumerGroup(stream: String, group: String, startId: String = "0") {
        try {
            redis.get().xgroupCreate(
                XReadArgs.StreamOffset.from(stream, startId),
                group,
                XGroupCreateArgs.Builder.mkstream()
            ).await()
        } catch (e: Exception) {
            if (e.message?.contains("BUSYGROUP") != true) throw e
        }
    }

    fun consume(stream: String, group: String, consumer: String): Flow<StreamMessage> = flow {
        while (true) {
            val messages = redis.get().xreadgroup(
                Consumer.from(group, consumer),
                XReadArgs.Builder.block(1000).count(10),
                XReadArgs.StreamOffset.from(stream, ">")
            ).await()

            messages?.forEach { message ->
                val id = message.id
                val data = message.body["data"] ?: ""
                emit(StreamMessage(id, data))
                redis.get().xack(stream, group, id).await()
            }
        }
    }
}

data class StreamMessage(val id: String, val data: String)