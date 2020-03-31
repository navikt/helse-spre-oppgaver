package no.nav.helse

import java.util.UUID

data class Oppgave(
    val hendelseId: UUID,
    val dokumentId: UUID,
    val tilstand: DatabaseTilstand,
    val dokumentType: DokumentType
)

enum class DokumentType {
    Inntektsmelding, Søknad
}
