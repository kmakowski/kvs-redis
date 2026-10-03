package kmakowski.kvs.redis

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import java.util.UUID

fun randomId() = UUID.randomUUID().toString()

fun test(block: suspend CoroutineScope.() -> Unit) {
    runBlocking {
        block()
    }
}
