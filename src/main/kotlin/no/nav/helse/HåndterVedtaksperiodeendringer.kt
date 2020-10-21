package no.nav.helse

import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.Oppgave.Tilstand
import no.nav.helse.Oppgave.Tilstand.*
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.LocalDateTime
import java.util.*

class HåndterVedtaksperiodeendringer(
    private val rapidsConnection: RapidsConnection,
    private val oppgaveDAO: OppgaveDAO,
    private val oppgaveProducer: KafkaProducer<String, OppgaveDTO>
) : River.PacketListener, Oppgave.Observer {

    val oppgaveTopicName = "aapen-helse-spre-oppgaver"

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("hendelser") }
        }.register(this)
    }

    sealed class Hendelse {
        abstract fun accept(oppgave: Oppgave)

        object TilInfotrygd : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }

        object Avsluttet : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }

        object Lest : Hendelse() {
            override fun accept(oppgave: Oppgave) {
                oppgave.håndter(this)
            }
        }
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet["hendelser"]
            .map { UUID.fromString(it.asText()) }
            .mapNotNull { oppgaveDAO.finnOppgave(it) }
            .onEach { it.setObserver(this) }
            .forEach { oppgave ->
                when (packet["gjeldendeTilstand"].asText()) {
                    "TIL_INFOTRYGD" -> Hendelse.TilInfotrygd
                    "AVSLUTTET" -> Hendelse.Avsluttet
                    else -> Hendelse.Lest
                }.accept(oppgave)
            }
    }

    override fun lagre(oppgave: Oppgave) {
        oppgaveDAO.oppdaterTilstand(oppgave)
    }

    override fun publiser(oppgave: Oppgave) {
        oppgaveProducer.send(
            ProducerRecord(
                oppgaveTopicName, OppgaveDTO(
                    dokumentType = oppgave.dokumentType.toDTO(),
                    oppdateringstype = oppgave.tilstand.toDTO(),
                    dokumentId = oppgave.dokumentId,
                    timeout = LocalDateTime.now().plusDays(28)
                )
            )
        )
        rapidsConnection.publish(JsonMessage.newMessage(
            mapOf(
                "@event_name" to oppgave.tilstand.toEventName(),
                "@id" to UUID.randomUUID(),
                "dokumentId" to oppgave.dokumentId,
                "hendelseId" to oppgave.hendelseId
            )
        ).toJson())

        log.info("Publisert oppgave på ${oppgave.dokumentType.name} i tilstand: ${oppgave.tilstand} med ider: {}, {}",
            keyValue("hendelseId", oppgave.hendelseId),
            keyValue("dokumentId", oppgave.dokumentId)
        )
    }

    private fun Tilstand.toDTO(): OppdateringstypeDTO = when (this) {
        SpleisFerdigbehandlet -> OppdateringstypeDTO.Ferdigbehandlet
        LagOppgave -> OppdateringstypeDTO.Opprett
        SpleisLest -> OppdateringstypeDTO.Utsett
        DokumentOppdaget -> error("skal ikke legge melding på topic om at dokument er oppdaget")
    }

    private fun Tilstand.toEventName() : String = when(this) {
        SpleisFerdigbehandlet -> "oppgavestyring_ferdigbehandlet"
        LagOppgave -> "oppgavestyring_opprett"
        SpleisLest -> "oppgavestyring_utsatt"
        DokumentOppdaget -> error("skal ikke legge melding på topic om at dokument er oppdaget")
    }
}

