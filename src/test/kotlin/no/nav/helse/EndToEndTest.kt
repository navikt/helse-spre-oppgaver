package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.convertValue
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import no.nav.helse.rapids_rivers.inMemoryRapid
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var hikariConfig: HikariConfig
    private lateinit var dataSource: HikariDataSource
    private lateinit var oppgaveDAO: OppgaveDAO
    private val rapid = inMemoryRapid {}
    var captureslot = mutableListOf<ProducerRecord<String, OppgaveDTO>>()
    private val mockProducer = mockk<KafkaProducer<String, OppgaveDTO>> {
        every { send(capture(captureslot)) } returns mockk()
    }

    @BeforeAll
    fun setup() {
        embeddedPostgres = EmbeddedPostgres.builder().start()

        hikariConfig = HikariConfig().apply {
            this.jdbcUrl = embeddedPostgres.getJdbcUrl("postgres", "postgres")
            maximumPoolSize = 3
            minimumIdle = 1
            idleTimeout = 10001
            connectionTimeout = 1000
            maxLifetime = 30001
        }

        dataSource = HikariDataSource(hikariConfig)

        Flyway.configure()
            .dataSource(dataSource)
            .load()
            .migrate()

        oppgaveDAO = OppgaveDAO(dataSource)

        RegistrerInntektsmeldinger(rapid, oppgaveDAO)
        RegistrerSøknader(rapid, oppgaveDAO)
        HåndterVedtaksperiodeendringer(rapid, oppgaveDAO, mockProducer)

    }

    @BeforeEach
    fun reset() {
        captureslot.clear()
    }

    @Test
    fun `spleis håndterer et helt sykeforløp`() {
        val søknad1HendelseId = UUID.randomUUID()
        val søknad1DokumentId = UUID.randomUUID()
        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendSøknad(søknad1HendelseId, søknad1DokumentId)
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "AVVENTER_INNTEKTSMELDING_FERDIG_GAP")
        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(
            listOf(søknad1HendelseId, inntektsmeldingHendelseId),
            "AVVENTER_SIMULERING"
        )

        sendVedtaksperiodeEndret(
            listOf(søknad1HendelseId, inntektsmeldingHendelseId),
            "AVVENTER_VILKÅRSPRØVING_ARBEIDSGIVERSØKNAD"
        )

        sendVedtaksperiodeEndret(listOf(søknad1HendelseId, inntektsmeldingHendelseId), "AVSLUTTET")

        assertOppgave(OppdateringstypeDTO.Utsett, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[0].value())
        assertOppgave(
            OppdateringstypeDTO.Utsett,
            inntektsmeldingDokumentId,
            DokumentTypeDTO.Inntektsmelding,
            captureslot[1].value()
        )
        assertOppgave(
            OppdateringstypeDTO.Ferdigbehandlet,
            søknad1DokumentId,
            DokumentTypeDTO.Søknad,
            captureslot[2].value()
        )
        assertOppgave(
            OppdateringstypeDTO.Ferdigbehandlet,
            inntektsmeldingDokumentId,
            DokumentTypeDTO.Inntektsmelding,
            captureslot[3].value()
        )
        assertEquals(4, captureslot.size)
    }

    @Test
    fun `spleis replayer søknad`() {
        val søknad1HendelseId = UUID.randomUUID()
        val søknad1DokumentId = UUID.randomUUID()

        sendSøknad(søknad1HendelseId, søknad1DokumentId)
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "AVVENTER_INNTEKTSMELDING_FERDIG_GAP")
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "AVSLUTTET")

        sendSøknad(søknad1HendelseId, søknad1DokumentId)
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "AVVENTER_INNTEKTSMELDING_FERDIG_GAP")
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "AVSLUTTET")

        assertOppgave(OppdateringstypeDTO.Utsett, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[0].value())
        assertOppgave(OppdateringstypeDTO.Ferdigbehandlet, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[1].value())
        assertEquals(2, captureslot.size)
    }

    @Test
    fun `spleis gir opp behandling av søknad`() {
        val søknad1HendelseId = UUID.randomUUID()
        val søknad1DokumentId = UUID.randomUUID()

        sendSøknad(søknad1HendelseId, søknad1DokumentId)
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "TIL_INFOTRYGD")

        assertOppgave(OppdateringstypeDTO.Opprett, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[0].value())
        assertEquals(1, captureslot.size)
    }

    @Test
    fun `spleis gir opp behandling i vilkårsprøving`() {
        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_VILKÅRSPRØVING")
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD")

        assertOppgave(OppdateringstypeDTO.Utsett, inntektsmeldingDokumentId, DokumentTypeDTO.Inntektsmelding, captureslot[0].value())
        assertOppgave(OppdateringstypeDTO.Opprett, inntektsmeldingDokumentId, DokumentTypeDTO.Inntektsmelding, captureslot[1].value())
    }

    @Test
    fun `tåler meldinger som mangler kritiske felter`() = runBlocking {
        rapid.sendToListeners("{}")
        assertTrue(captureslot.isEmpty())
    }

    @Test
    fun `ignorerer endrede vedtaksperioder uten tidligere dokumenter`() {
        val inntektsmeldingHendelseId = UUID.randomUUID()
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_VILKÅRSPRØVING")

        assertTrue(captureslot.isEmpty())
    }

    private fun assertOppgave(
        oppdateringstypeDTO: OppdateringstypeDTO,
        dokumentId: UUID,
        dokumentType: DokumentTypeDTO,
        oppgaveDTO: OppgaveDTO
    ) {
        assertEquals(dokumentId, oppgaveDTO.dokumentId)
        assertEquals(dokumentType, oppgaveDTO.dokumentType)
        assertEquals(oppdateringstypeDTO, oppgaveDTO.oppdateringstype)
    }

    fun sendSøknad(hendelseId: UUID, dokumentId: UUID = UUID.randomUUID()) {
        rapid.sendToListeners(sendtSøknad(hendelseId, dokumentId))
    }

    fun sendInntektsmelding(hendelseId: UUID, dokumentId: UUID) {
        rapid.sendToListeners(inntektsmelding(hendelseId, dokumentId))
    }

    fun sendVedtaksperiodeEndret(hendelseIder: List<UUID>, tilstand: String) {
        rapid.sendToListeners(vedtaksperiodeEndret(hendelseIder, tilstand))
    }
}


fun vedtaksperiodeEndret(
    hendelseIder: List<UUID>,
    gjeldendeTilstand: String
) =
    """{
            "@event_name": "vedtaksperiode_endret",
            "hendelsesIder": ${hendelseIder.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }},
            "gjeldendeTilstand": "$gjeldendeTilstand",
            "vedtaksperiodeId": "${UUID.randomUUID()}"
        }"""



