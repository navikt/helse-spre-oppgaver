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
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
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
    val datasource = DataSourceBuilder(System.getenv())
        .apply(DataSourceBuilder::migrate)
        .getDataSource()

    val server = embeddedServer(Netty, 8080) {
        install(MicrometerMetrics) {
            registry = meterRegistry
        }

        routing {
            registerHealthApi({ true }, { true }, meterRegistry)
        }
    }.start(wait = false)

    val rapidConsumer =
        KafkaConsumer<String, JsonNode?>(loadBaseConfig(environment, serviceUser).toConsumerConfig())
    val oppgaveProducer =
        KafkaProducer<String, OppgaveDTO>(loadBaseConfig(environment, serviceUser).toProducerConfig())
    val oppgaveDAO = OppgaveDAO(datasource)

    rapidConsumer
        .subscribe(listOf(environment.rapidTopic))

    rapidConsumer.asFlow()
        .catch {
            server.stop(10, 10, TimeUnit.SECONDS)
            throw it
        }
        .oppgaveFlow(oppgaveDAO = oppgaveDAO)
        .collect { oppgaveProducer.send(ProducerRecord(environment.spreoppgaverTopic, it)) }

    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop(10, 10, TimeUnit.SECONDS)
    })
}

fun Flow<Pair<String, JsonNode?>>.oppgaveFlow(oppgaveDAO: OppgaveDAO) = this
    .map { (_, value) -> value }
    .filterNotNull()
    .filter { it["@event_name"]?.asText() in listOf("sendt_søknad_nav", "inntektsmelding", "vedtaksperiode_endret") }
    .onEach {
        log.info(
            "Innkommende hendelse med {} og {}",
            keyValue("type", it["@event_name"].asText()),
            keyValue("referanse", it["@id"]?.asText() ?: it["vedtaksperiodeId"].asText())
        )
    }
    .flatMapConcat { hendelse(it).asFlow() }
    .map { håndter(it, oppgaveDAO) }
    .filterNotNull()
    .onEach {
        log.info(
            "Oppgave har endret status: {}, {}, {}, timeout: ${it.timeout}",
            keyValue("dokumentId", it.dokumentId),
            keyValue("oppdateringstype", it.oppdateringstype),
            keyValue("dokumenttype", it.dokumentType)
        )
    }

fun håndter(hendelse: Hendelse, oppgaveDAO: OppgaveDAO): OppgaveDTO? {
    return when (hendelse) {
        is Hendelse.NyttDokument -> {
            if (oppgaveDAO.finnOppgave(hendelse.hendelseId) == null) {
                oppgaveDAO.opprettOppgave(hendelse.hendelseId, hendelse.dokumentId, hendelse.dokumentType)
            }
            null
        }
        is Hendelse.Tilstandsendring -> {
            val oppgave = oppgaveDAO.finnOppgave(hendelse.hendelseId) ?: return null
            if (oppgave.tilstand.godtarOvergang(hendelse.tilTilstand)) {
                oppgaveDAO.oppdaterTilstand(hendelse.hendelseId, hendelse.tilTilstand)
                OppgaveDTO(
                    dokumentType = oppgave.dokumentType.toDTO(),
                    oppdateringstype = hendelse.tilTilstand.toDTO(),
                    dokumentId = oppgave.dokumentId,
                    timeout = LocalDateTime.now().plusDays(14)
                )
            } else {
                null
            }
        }
    }
}

private fun DatabaseTilstand.toDTO(): OppdateringstypeDTO = when (this) {
    DatabaseTilstand.SpleisFerdigbehandlet -> OppdateringstypeDTO.Ferdigbehandlet
    DatabaseTilstand.LagOppgave -> OppdateringstypeDTO.Opprett
    DatabaseTilstand.SpleisLest -> OppdateringstypeDTO.Utsett
    else -> error("skal ikke legge melding på topic om at dokument er oppdaget")
}


fun hendelse(node: JsonNode) = when (val eventType = node["@event_name"].asText()) {
    "sendt_søknad_nav" -> listOf(
        Hendelse.NyttDokument(
            hendelseId = UUID.fromString(node["@id"].asText()),
            dokumentId = UUID.fromString(node["id"].asText()),
            dokumentType = DokumentType.Søknad
        )
    )
    "inntektsmelding" -> listOf(
        Hendelse.NyttDokument(
            hendelseId = UUID.fromString(node["@id"].asText()),
            dokumentId = UUID.fromString(node["inntektsmeldingId"].asText()),
            dokumentType = DokumentType.Inntektsmelding
        )
    )
    "vedtaksperiode_endret" -> node["hendelser"].map {
        Hendelse.Tilstandsendring(
            hendelseId = UUID.fromString(it.asText()),
            tilstand = node["gjeldendeTilstand"].asText()
        )
    }
    else -> error("Prøver å tolke en event-type vi ikke forstår: $eventType")
}

sealed class Hendelse {
    abstract val tilTilstand: DatabaseTilstand

    data class Tilstandsendring(
        val hendelseId: UUID,
        val tilstand: String
    ) : Hendelse() {
        override val tilTilstand: DatabaseTilstand = when (tilstand) {
            "TIL_INFOTRYGD" -> DatabaseTilstand.LagOppgave
            "AVSLUTTET" -> DatabaseTilstand.SpleisFerdigbehandlet
            else -> DatabaseTilstand.SpleisLest
        }
    }

    data class NyttDokument(
        val hendelseId: UUID,
        val dokumentId: UUID,
        val dokumentType: DokumentType
    ) : Hendelse() {
        override val tilTilstand: DatabaseTilstand = DatabaseTilstand.DokumentOppdaget
    }
}
