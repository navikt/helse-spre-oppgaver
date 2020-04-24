package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.HåndterVedtaksperiodeendringer.*
import no.nav.helse.rapids_rivers.RapidsConnection
import java.util.UUID

internal class TestRapid : RapidsConnection() {
    private companion object {
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    private val context = TestContext()
    private val messages = mutableListOf<Pair<String?, String>>()
    internal val inspektør get() = RapidInspektør(messages.toList())

    internal fun reset() {
        messages.clear()
    }

    fun sendTestMessage(message: String) {
        listeners.forEach { it.onMessage(message, context) }
    }

    override fun publish(message: String) {
        messages.add(null to message)
    }

    override fun publish(key: String, message: String) {
        messages.add(key to message)
    }

    override fun start() {}

    override fun stop() {}

    private inner class TestContext : MessageContext {
        override fun send(message: String) {
            publish(message)
        }

        override fun send(key: String, message: String) {
            publish(key, message)
        }
    }

    class RapidInspektør(private val messages: List<Pair<String?, String>>) {
        private val jsonmeldinger = mutableMapOf<Int, JsonNode>()

        fun events(name: String, hendelseId: UUID) = messages
            .mapIndexed { indeks, _ -> melding(indeks) }
            .filter { it["@event_name"].textValue() == name && it["hendelseId"].textValue() == hendelseId.toString() }

        fun events() = messages
            .mapIndexed { indeks, _ -> melding(indeks) }

        fun melding(indeks: Int) = jsonmeldinger.getOrPut(indeks) { objectMapper.readTree(messages[indeks].second) }
    }
}
