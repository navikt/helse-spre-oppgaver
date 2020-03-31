package no.nav.helse

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.install
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val objectMapper: ObjectMapper = jacksonObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log: Logger = LoggerFactory.getLogger("spreoppgaver")

@ExperimentalCoroutinesApi
@FlowPreview
fun main() = runBlocking(Executors.newFixedThreadPool(4).asCoroutineDispatcher()) {
    val serviceUser = readServiceUserCredentials()
    val environment = setUpEnvironment()

    val server = embeddedServer(Netty, 8080) {
        install(MicrometerMetrics) {
            registry = meterRegistry
        }

        routing {
            registerHealthApi({ true }, { true }, meterRegistry)
        }
    }.start(wait = false)

    val rapidConsumer =
        KafkaConsumer<ByteArray, JsonNode?>(loadBaseConfig(environment, serviceUser).toConsumerConfig())
    val oppgaveProducer =
        KafkaProducer<String, OppgaveDTO>(loadBaseConfig(environment, serviceUser).toProducerConfig())

    rapidConsumer
        .subscribe(listOf(environment.rapidTopic))

    rapidConsumer.asFlow()
        .catch {
            server.stop(10, 10, TimeUnit.SECONDS)
            throw it
        }

    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop(10, 10, TimeUnit.SECONDS)
    })
}

fun Flow<Pair<String, JsonNode>>.oppgaveFlow(oppgaveDAO: OppgaveDAO) = this
    .map { (_, value) -> value }
    .filterNotNull()
    .filter { it["@event_type"].asText() in listOf("sendt_søknad_nav", "inntektsmelding", "vedtaksperiode_endret") }
    .flatMapConcat { konverterTilRiverInput(it).asFlow() }
    .map { håndter(it, oppgaveDAO) }
    .filterNotNull()

fun håndter(input: RiverInput, oppgaveDAO: OppgaveDAO): Oppgave? {
    return when (input) {
        is RiverInput.NyttDokument -> {
            if (oppgaveDAO.finnOppgave(input.hendelseId) == null) {
                oppgaveDAO.opprettOppgave(input.hendelseId, input.dokumentId)
            }
            null
        }
        is RiverInput.Tilstandsendring -> {
            val oppgave = oppgaveDAO.finnOppgave(input.hendelseId) ?: return null
            if (oppgave.tilstand.godtarOvergang(input.tilTilstand)) {
                oppgaveDAO.oppdaterTilstand(input.hendelseId, input.tilTilstand)
                oppgave.copy(tilstand = input.tilTilstand)
            } else {
                null
            }
        }
    }
}

fun konverterTilRiverInput(node: JsonNode): List<RiverInput> {
    val eventType = node["@event_type"].asText()
    return when (eventType) {
        "sendt_søknad_nav" -> listOf(RiverInput.NyttDokument(
            hendelseId = UUID.fromString(node["@id"].asText()),
            dokumentId = UUID.fromString(node["id"].asText())
        ))
        "inntektsmelding" -> listOf(RiverInput.NyttDokument(
            hendelseId = UUID.fromString(node["@id"].asText()),
            dokumentId = UUID.fromString(node["inntektsmeldingId"].asText())
        ))
        "vedtaksperiode_endret" -> node["hendelser"].map {
            RiverInput.Tilstandsendring(
                hendelseId = UUID.fromString(it.asText()),
                tilstand = node["gjeldendeTilstand"].asText()
            )
        }
        else -> error("Prøver å tolke en event-type vi ikke forstår: $eventType")
    }
}

sealed class RiverInput {
    abstract val tilTilstand: DatabaseTilstand

    data class Tilstandsendring(
        val hendelseId: UUID,
        val tilstand: String
    ) : RiverInput() {
        override val tilTilstand: DatabaseTilstand = when (tilstand) {
            "AVSLUTTET" -> DatabaseTilstand.SpleisFerdigbehandlet
            else -> DatabaseTilstand.SpleisLest
        }
    }

    data class NyttDokument(
        val hendelseId: UUID,
        val dokumentId: UUID
    ) : RiverInput() {
        override val tilTilstand: DatabaseTilstand = DatabaseTilstand.DokumentOppdaget
    }
}

data class OppgaveDTO(
    val type: String,
    val dokumentId: UUID
)
