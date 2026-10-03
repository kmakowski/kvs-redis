package kmakowski.kvs.redis

import kmakowski.kvs.CachingKvs
import kmakowski.kvs.inmemory.InMemoryKeyValueStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CachingKvsTest {

    @Test
    fun putStringValue_writesToTarget() = test {
        val kvs = CachingKvs(redisKvs(), InMemoryKeyValueStore())

        val key = randomId()
        kvs.putStringValue(key, "test-value")
        assertThat(kvs.getObjectAsString(key)).isEqualTo("test-value")
    }

    @Test
    fun putObjectAsJson_writesToTarget() = test {
        val target = inMemoryKeyValueStore()
        val kvs = CachingKvs(redisKvs(), target)

        val testObject = mapOf("key" to "value")
        val key = randomId()
        kvs.putObjectAsJson(key, testObject)
        assertThat(kvs.getObject(key, Map::class.java)).isEqualTo(testObject)

        assertThat(target.getObjectAsString(key)).isEqualTo("""{"key":"value"}""")
    }

    @Test
    fun addListItem_addsItemToList() = test {
        val target = InMemoryKeyValueStore()
        val kvs = CachingKvs(redisKvs(), target)

        val key = randomId()
        
        kvs.addListItem(key, "someList", "someItem1")
        kvs.addListItem(key, "someList", "someItem2")
        val items = kvs.getListItems(key, "someList")
        assertThat(items).containsExactlyInAnyOrder("someItem1", "someItem2")
        assertThat(target.getListItems(key, "someList")).containsExactlyInAnyOrder("someItem1", "someItem2")
    }

    private fun inMemoryKeyValueStore() = InMemoryKeyValueStore()


    private fun redisKvs() = RedisKvs(RedisHolder("redis://localhost:6379"))

}