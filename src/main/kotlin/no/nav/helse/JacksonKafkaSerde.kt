package no.nav.helse

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class JacksonDeserializer : Deserializer<JsonNode?> {
    override fun deserialize(topic: String?, data: ByteArray) = try {
        objectMapper.readTree(data)
    } catch (exception: JsonParseException) {
        null
    }
}


class JacksonSerializer<T> : Serializer<T> {
    override fun serialize(topic: String?, data: T): ByteArray = objectMapper.writeValueAsBytes(data)
}
