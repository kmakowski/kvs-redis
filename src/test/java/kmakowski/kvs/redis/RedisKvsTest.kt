package kmakowski.kvs.redis

import kmakowski.kvs.PrefixedKeyValueStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RedisKvsTest {
    
    data class SomeClass(val key1: String, val key2: String)
    
    @Test
    fun name1() = test {
        val kvs = PrefixedKeyValueStore("accounts", RedisKvs(RedisHolder(redisUrl())))
        val objectAsString = kvs.getObjectAsString("72bc3bdd-6b94-4cd7-b826-7c0a1b610443")
        println("coroutineContext = $objectAsString")
    }
    
    @Test
    fun shouldWriteAndRead() = test {
        val kvs = RedisKvs(RedisHolder(redisUrl()))
        kvs.putStringValue("krz", "makowski")
        val result = kvs.getObjectAsString("krz")
        assertThat(result).isEqualTo("makowski")

        (1..4).forEach {
            kvs.addListItem("someKey", "ids", "item_$it")
        }
        kvs.deleteListItem("someKey", "ids", "item_4")
        val listItems = kvs.getListItems("someKey", "ids")

        assertThat(listItems).containsExactlyInAnyOrder("item_1", "item_2", "item_3")
        
        kvs.putObjectAsJson("someKey2", SomeClass("value1", "value2"))
        val res = kvs.getObject("someKey2", SomeClass::class.java)
        assertThat(res).isEqualTo(SomeClass("value1", "value2"))
    }

    private fun redisUrl(): String {
        val redisHost = System.getenv("REDIS_HOST") ?: "localhost"
        val redisPort = System.getenv("REDIS_PORT") ?: "6379"

        return "redis://$redisHost:$redisPort"
    }
}