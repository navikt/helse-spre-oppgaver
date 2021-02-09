package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*

class HåndterInntektsmeldingLagtPåKjøl(
    rapidsConnection: RapidsConnection,
    private val oppgaveDAO: OppgaveDAO,
    oppgaveProducer: KafkaProducer<String, OppgaveDTO>
) : River.PacketListener {

    private val observer = OppgaveObserver(oppgaveDAO, oppgaveProducer, rapidsConnection)

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "inntektsmelding_lagt_på_kjøl") }
            validate { it.requireKey("hendelseId") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        UUID.fromString(packet["hendelseId"].asText()).let { hendelseId ->
            oppgaveDAO.finnOppgave(hendelseId)?.setObserver(observer)?.let { oppgave ->
                Hendelse.Lest.accept(oppgave)
            }
        }
    }
}

