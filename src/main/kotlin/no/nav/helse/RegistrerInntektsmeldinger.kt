package no.nav.helse

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.util.UUID

class RegistrerInntektsmeldinger(rapidsConnection: RapidsConnection, private val oppgaveDAO: OppgaveDAO): River.PacketListener{
    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("@id") }
            validate { it.requireKey("inntektsmeldingId") }
            validate { it.requireValue("@event_name", "inntektsmelding") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val hendelseId = UUID.fromString(packet["@id"].asText())
        val dokumentId = UUID.fromString(packet["inntektsmeldingId"].asText())

        if (oppgaveDAO.finnOppgave(hendelseId) == null) {
            oppgaveDAO.opprettOppgave(hendelseId, dokumentId, DokumentType.Inntektsmelding)
        }
    }
}
