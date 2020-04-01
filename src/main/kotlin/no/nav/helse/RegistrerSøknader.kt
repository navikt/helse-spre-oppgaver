package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.util.UUID

class RegistrerSøknader(rapidsConnection: RapidsConnection, private val oppgaveDAO: OppgaveDAO) : River.PacketListener{

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("@id") }
            validate { it.requireKey("id") }
            validate { it.requireValue("@event_name", "sendt_søknad_nav") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val hendelseId = UUID.fromString(packet["@id"].asText())
        val dokumentId = UUID.fromString(packet["id"].asText())

        if (oppgaveDAO.finnOppgave(hendelseId) == null) {
            oppgaveDAO.opprettOppgave(hendelseId, dokumentId, DokumentType.Søknad)
        }
    }
}
