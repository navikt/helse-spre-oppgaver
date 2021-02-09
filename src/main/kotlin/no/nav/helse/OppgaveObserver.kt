package no.nav.helse

import net.logstash.logback.argument.StructuredArguments
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.LocalDateTime
import java.util.*

class OppgaveObserver(
    private val oppgaveDAO: OppgaveDAO,
    private val oppgaveProducer: KafkaProducer<String, OppgaveDTO>,
    private val rapidsConnection: RapidsConnection
) : Oppgave.Observer {

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
                    timeout = LocalDateTime.now().plusDays(50)
                )
            )
        )
        rapidsConnection.publish(
            JsonMessage.newMessage(
                mapOf(
                    "@event_name" to oppgave.tilstand.toEventName(),
                    "@id" to UUID.randomUUID(),
                    "dokumentId" to oppgave.dokumentId,
                    "hendelseId" to oppgave.hendelseId
                )
            ).toJson()
        )

        log.info(
            "Publisert oppgave på ${oppgave.dokumentType.name} i tilstand: ${oppgave.tilstand} med ider: {}, {}",
            StructuredArguments.keyValue("hendelseId", oppgave.hendelseId),
            StructuredArguments.keyValue("dokumentId", oppgave.dokumentId)
        )
    }

    private fun Oppgave.Tilstand.toDTO(): OppdateringstypeDTO = when (this) {
        Oppgave.Tilstand.KortPeriodeFerdigbehandlet -> OppdateringstypeDTO.Ferdigbehandlet
        Oppgave.Tilstand.SpleisFerdigbehandlet -> OppdateringstypeDTO.Ferdigbehandlet
        Oppgave.Tilstand.LagOppgave -> OppdateringstypeDTO.Opprett
        Oppgave.Tilstand.SpleisLest -> OppdateringstypeDTO.Utsett
        Oppgave.Tilstand.DokumentOppdaget -> error("skal ikke legge melding på topic om at dokument er oppdaget")
    }

    private fun Oppgave.Tilstand.toEventName(): String = when (this) {
        Oppgave.Tilstand.SpleisFerdigbehandlet -> "oppgavestyring_ferdigbehandlet"
        Oppgave.Tilstand.LagOppgave -> "oppgavestyring_opprett"
        Oppgave.Tilstand.SpleisLest -> "oppgavestyring_utsatt"
        Oppgave.Tilstand.KortPeriodeFerdigbehandlet -> "oppgavestyring_kort_periode"
        Oppgave.Tilstand.DokumentOppdaget -> error("skal ikke legge melding på topic om at dokument er oppdaget")
    }
}
