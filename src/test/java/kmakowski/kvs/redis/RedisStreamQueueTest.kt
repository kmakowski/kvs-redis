package kmakowski.kvs.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class RedisStreamQueueTest {
    
    private lateinit var streamQueue: RedisStreamQueue
    private lateinit var streamName: String
    private lateinit var groupName: String
    private lateinit var consumerName: String
    
    @BeforeEach
    fun setup() {
        val redisUrl = redisUrl()
        val redisClient = RedisClient.create(RedisURI.create(redisUrl))
        streamQueue = RedisStreamQueue(RedisHolder(redisUrl))
        
        // Use unique names for each test to avoid conflicts
        streamName = "test-stream-${randomId()}"
        groupName = "test-group-${randomId()}"
        consumerName = "test-consumer-${randomId()}"
    }
    
    @Test
    fun shouldPublishMessageToStream() = test {
        // Given
        val testMessage = TestMessage("Hello", "World")
        
        // When
        val messageId = streamQueue.publish(streamName, testMessage)
        
        // Then
        assertThat(messageId).isNotNull()
        assertThat(messageId).isNotEmpty()
    }
    
    @Test
    fun shouldCreateConsumerGroup() = test {
        // Given
        streamQueue.publish(streamName, TestMessage("init", "message"))
        
        // When
        streamQueue.createConsumerGroup(streamName, groupName)
        
        // Then - no exception should be thrown
        // Creating the same group again should not throw (BUSYGROUP error is caught)
        streamQueue.createConsumerGroup(streamName, groupName)
    }
    
    @Test
    fun shouldConsumePublishedMessages() = test {
        // Given
        val testMessage1 = TestMessage("key1", "value1")
        val testMessage2 = TestMessage("key2", "value2")
        
        // Publish messages first
        streamQueue.publish(streamName, testMessage1)
        streamQueue.publish(streamName, testMessage2)
        
        // Create consumer group
        streamQueue.createConsumerGroup(streamName, groupName, "0")
        
        // When - consume messages
        val messages = mutableListOf<StreamMessage>()
        val job = launch {
            streamQueue.consume(streamName, groupName, consumerName)
                .take(2)
                .collect { messages.add(it) }
        }
        
        // Wait for messages to be consumed
        withTimeout(5000) {
            job.join()
        }
        
        // Then
        assertThat(messages).hasSize(2)
        assertThat(messages[0].data).contains("key1")
        assertThat(messages[1].data).contains("key2")
    }
    
    @Test
    fun shouldConsumeNewMessagesAfterGroupCreation() = test {
        // Given
        val testMessage = TestMessage("test", "data")
        
        // Create consumer group first
        streamQueue.createConsumerGroup(streamName, groupName, "$")
        
        // Start consuming in background
        val messages = mutableListOf<StreamMessage>()
        val job = launch {
            streamQueue.consume(streamName, groupName, consumerName)
                .take(1)
                .collect { messages.add(it) }
        }
        
        // Give consumer time to start
        delay(100)
        
        // When - publish message after consumer started
        streamQueue.publish(streamName, testMessage)
        
        // Wait for message to be consumed
        withTimeout(5000) {
            job.join()
        }
        
        // Then
        assertThat(messages).hasSize(1)
        assertThat(messages[0].data).contains("test")
    }
    
    @Test
    fun shouldHandleMultipleConsumersInSameGroup() = test {
        // Given
        val testMessage1 = TestMessage("msg1", "data1")
        val testMessage2 = TestMessage("msg2", "data2")
        
        streamQueue.createConsumerGroup(streamName, groupName, "$")
        
        // Start two consumers
        val consumer1Messages = mutableListOf<StreamMessage>()
        val consumer2Messages = mutableListOf<StreamMessage>()
        
        val job1 = launch {
            streamQueue.consume(streamName, groupName, "consumer1")
                .take(1)
                .collect { consumer1Messages.add(it) }
        }
        
        val job2 = launch {
            streamQueue.consume(streamName, groupName, "consumer2")
                .take(1)
                .collect { consumer2Messages.add(it) }
        }
        
        delay(100)
        
        // When - publish two messages
        streamQueue.publish(streamName, testMessage1)
        streamQueue.publish(streamName, testMessage2)
        
        // Wait for both consumers
        withTimeout(5000) {
            job1.join()
            job2.join()
        }
        
        // Then - each consumer should get one message (load balanced)
        val totalMessages = consumer1Messages.size + consumer2Messages.size
        assertThat(totalMessages).isEqualTo(2)
    }
    
    @Test
    fun shouldAcknowledgeMessages() = test {
        // Given
        val testMessage = TestMessage("ack", "test")

        streamQueue.publish(streamName, testMessage)
        streamQueue.createConsumerGroup(streamName, groupName, "0")

        // When - consume and acknowledge
        val messages = streamQueue.consume(streamName, groupName, consumerName)
            .take(1)
            .toList()

        // Then - message should be consumed and acknowledged
        assertThat(messages).hasSize(1)
        assertThat(messages[0].data).contains("ack")

        // Publish a new message first
        streamQueue.publish(streamName, TestMessage("new", "message"))

        // If we try to consume again with a new consumer, we shouldn't get the acknowledged message
        // (because it was acknowledged), only the new one
        val newConsumerMessages = mutableListOf<StreamMessage>()
        val job = launch {
            streamQueue.consume(streamName, groupName, "consumer2")
                .take(1)
                .collect { newConsumerMessages.add(it) }
        }

        withTimeout(5000) {
            job.join()
        }

        // Should only get the new message, not the acknowledged one
        assertThat(newConsumerMessages).hasSize(1)
        assertThat(newConsumerMessages[0].data).contains("new")
    }
    
    @Test
    fun shouldPropagateErrorWhenConsumingFromNonExistentGroup() = test {
        // Given - a stream exists but no consumer group has been created
        streamQueue.publish(streamName, TestMessage("orphan", "message"))

        // When / Then - consuming from a non-existent group should propagate an exception
        assertThrows<Exception> {
            streamQueue.consume(streamName, groupName, consumerName)
                .take(1)
                .toList()
        }
    }

    private fun redisUrl(): String {
        val redisHost = System.getenv("REDIS_HOST") ?: "localhost"
        val redisPort = System.getenv("REDIS_PORT") ?: "6379"
        return "redis://$redisHost:$redisPort"
    }
    
    data class TestMessage(val key: String, val value: String)
}

