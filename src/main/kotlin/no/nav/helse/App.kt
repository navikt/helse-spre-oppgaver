package no.nav.helse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

internal val objectMapper: ObjectMapper = jacksonObjectMapper()
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())

internal val log = LoggerFactory.getLogger("helse-spre-oppgaver")
internal val oppgaveTopicName = "aapen-helse-spre-oppgaver"

fun main() {
    val rapidsConnection = launchApplication(System.getenv())
    rapidsConnection.start()
}

fun launchApplication(
    environment: Map<String, String>
): RapidsConnection {
    val serviceUser = readServiceUserCredentials()
    val datasource = DataSourceBuilder(System.getenv())
        .apply(DataSourceBuilder::migrate)
        .getDataSource()

    val oppgaveDAO = OppgaveDAO(datasource)
    val oppgaveProducer =
        KafkaProducer<String, OppgaveDTO>(
            loadBaseConfig(
                environment.getValue("KAFKA_BOOTSTRAP_SERVERS"),
                serviceUser
            ).toProducerConfig()
        )

    return RapidApplication.create(environment).apply {
        RegistrerSøknader(this, oppgaveDAO)
        RegistrerInntektsmeldinger(this, oppgaveDAO)
        HåndterVedtaksperiodeendringer(this, oppgaveDAO, oppgaveProducer)
    }
}

