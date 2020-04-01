package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.LocalDateTime
import java.util.UUID

class HåndterVedtaksperiodeendringer(
    rapidsConnection: RapidsConnection,
    private val oppgaveDAO: OppgaveDAO,
    private val oppgaveProducer: KafkaProducer<String, OppgaveDTO>
) : River.PacketListener {

    val oppgaveTopicName = "aapen-helse-spre-oppgaver"

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("hendelsesIder") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet["hendelsesIder"].map { it ->
            val hendelseId = UUID.fromString(it.asText())
            val oppgave = oppgaveDAO.finnOppgave(hendelseId)
            val gjeldendeTilstand = packet["gjeldendeTilstand"].asText()
            val tilstandsendring = Tilstandsendring(hendelseId, gjeldendeTilstand)

            oppgave?.let {
                if (it.tilstand.godtarOvergang(tilstandsendring.tilTilstand)) {
                    oppgaveDAO.oppdaterTilstand(hendelseId, tilstandsendring.tilTilstand)
                    oppgaveProducer.send(
                        ProducerRecord(
                            oppgaveTopicName, OppgaveDTO(
                                dokumentType = oppgave.dokumentType.toDTO(),
                                oppdateringstype = tilstandsendring.tilTilstand.toDTO(),
                                dokumentId = oppgave.dokumentId,
                                timeout = LocalDateTime.now().plusDays(14)
                            )
                        )
                    )
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


    class Tilstandsendring(
        val hendelseId: UUID,
        tilstand: String
    ) {
        val tilTilstand: DatabaseTilstand = when (tilstand) {
            "TIL_INFOTRYGD" -> DatabaseTilstand.LagOppgave
            "AVSLUTTET" -> DatabaseTilstand.SpleisFerdigbehandlet
            else -> DatabaseTilstand.SpleisLest
        }
    }
}
