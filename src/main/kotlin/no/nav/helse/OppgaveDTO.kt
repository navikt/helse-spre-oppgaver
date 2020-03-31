package no.nav.helse

import java.time.LocalDateTime
import java.util.UUID

data class OppgaveDTO(
    val dokumentType: DokumentTypeDTO,
    val oppdateringstype: OppdateringstypeDTO,
    val dokumentId: UUID,
    val timeout: LocalDateTime? = null
)

fun DokumentType.toDTO(): DokumentTypeDTO = when (this) {
    DokumentType.Inntektsmelding -> DokumentTypeDTO.Inntektsmelding
    DokumentType.Søknad -> DokumentTypeDTO.Søknad
}

enum class OppdateringstypeDTO {
    Utsett, Opprett, Ferdigbehandlet
}

enum class DokumentTypeDTO {
    Inntektsmelding, Søknad
}
