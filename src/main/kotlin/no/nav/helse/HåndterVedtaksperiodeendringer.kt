package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*

class HåndterVedtaksperiodeendringer(
    rapidsConnection: RapidsConnection,
    private val oppgaveDAO: OppgaveDAO,
    oppgaveProducer: KafkaProducer<String, OppgaveDTO>
) : River.PacketListener {

    private val observer = OppgaveObserver(oppgaveDAO, oppgaveProducer, rapidsConnection)

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("hendelser") }
        }.register(this)
    }


    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet["hendelser"]
            .map { UUID.fromString(it.asText()) }
            .mapNotNull { oppgaveDAO.finnOppgave(it) }
            .onEach { it.setObserver(observer) }
            .forEach { oppgave ->
                when (packet["gjeldendeTilstand"].asText()) {
                    "TIL_INFOTRYGD" -> Hendelse.TilInfotrygd
                    "AVSLUTTET" -> Hendelse.Avsluttet
                    "AVSLUTTET_UTEN_UTBETALING" -> Hendelse.Avsluttet
                    "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING" -> Hendelse.AvsluttetUtenUtbetalingMedInntektsmelding
                    else -> Hendelse.Lest
                }.accept(oppgave)
            }
    }
}

