package no.nav.helse

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import no.nav.helse.DokumentTypeDTO.Inntektsmelding
import no.nav.helse.OppdateringstypeDTO.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var hikariConfig: HikariConfig
    private lateinit var dataSource: HikariDataSource
    private lateinit var oppgaveDAO: OppgaveDAO
    private val rapid = TestRapid()
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

        rapid.registerRivers(oppgaveDAO, mockProducer)
    }

    @BeforeEach
    fun reset() {
        captureslot.clear()
        rapid.reset()
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

        assertOppgave(Utsett, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[0].value())
        assertOppgave(
            Utsett,
            inntektsmeldingDokumentId,
            Inntektsmelding,
            captureslot[1].value()
        )
        assertOppgave(
            Ferdigbehandlet,
            søknad1DokumentId,
            DokumentTypeDTO.Søknad,
            captureslot[2].value()
        )
        assertOppgave(
            Ferdigbehandlet,
            inntektsmeldingDokumentId,
            Inntektsmelding,
            captureslot[3].value()
        )
        assertEquals(4, captureslot.size)

        assertEquals(4, rapid.inspektør.events().size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_utsatt", søknad1HendelseId).size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_ferdigbehandlet", søknad1HendelseId).size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_utsatt", inntektsmeldingHendelseId).size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_ferdigbehandlet", inntektsmeldingHendelseId).size)
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

        assertOppgave(Utsett, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[0].value())
        assertOppgave(
            Ferdigbehandlet,
            søknad1DokumentId,
            DokumentTypeDTO.Søknad,
            captureslot[1].value()
        )
        assertEquals(2, captureslot.size)

        assertEquals(2, rapid.inspektør.events().size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_utsatt", søknad1HendelseId).size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_ferdigbehandlet", søknad1HendelseId).size)
    }

    @Test
    fun `spleis gir opp behandling av søknad`() {
        val søknad1HendelseId = UUID.randomUUID()
        val søknad1DokumentId = UUID.randomUUID()

        sendSøknad(søknad1HendelseId, søknad1DokumentId)
        sendVedtaksperiodeEndret(listOf(søknad1HendelseId), "TIL_INFOTRYGD")

        assertOppgave(Opprett, søknad1DokumentId, DokumentTypeDTO.Søknad, captureslot[0].value())
        assertEquals(1, captureslot.size)

        assertEquals(1, rapid.inspektør.events().size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_opprett", søknad1HendelseId).size)
    }

    @Test
    fun `spleis gir opp behandling i vilkårsprøving`() {
        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_VILKÅRSPRØVING")
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD")

        assertOppgave(
            Utsett,
            inntektsmeldingDokumentId,
            Inntektsmelding,
            captureslot[0].value()
        )
        assertOppgave(
            Opprett,
            inntektsmeldingDokumentId,
            Inntektsmelding,
            captureslot[1].value()
        )

        assertEquals(2, rapid.inspektør.events().size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_utsatt", inntektsmeldingHendelseId).size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_opprett", inntektsmeldingHendelseId).size)
    }

    @Test
    fun `tåler meldinger som mangler kritiske felter`() = runBlocking {
        rapid.sendTestMessage("{}")
        assertTrue(captureslot.isEmpty())
        assertEquals(0, rapid.inspektør.events().size)
    }

    @Test
    fun `ignorerer endrede vedtaksperioder uten tidligere dokumenter`() {
        val inntektsmeldingHendelseId = UUID.randomUUID()
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_VILKÅRSPRØVING")

        assertTrue(captureslot.isEmpty())
        assertEquals(0, rapid.inspektør.events().size)
    }

    @Test
    fun `ignorerer avslutting av AG-søknad`() {
        val søknadArbeidsgiverHendelseId = UUID.randomUUID()
        val søknadArbeidsgiverDokumentId = UUID.randomUUID()

        sendArbeidsgiversøknad(søknadArbeidsgiverHendelseId, søknadArbeidsgiverDokumentId)
        sendVedtaksperiodeEndret(listOf(søknadArbeidsgiverHendelseId), "AVSLUTTET_UTEN_UTBETALING")

        assertTrue(captureslot.isEmpty())
    }

    @Test
    fun `vedtaksperiode avsluttes uten utbetaling med inntektsmelding`() {
        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        val søknadHendelseId = UUID.randomUUID()
        val søknadDokumentId = UUID.randomUUID()

        sendVedtaksperiodeEndret(listOf(søknadHendelseId), "MOTTATT_SYKMELDING_FERDIG_GAP")
        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_SØKNAD_FERDIG_GAP")
        sendSøknad(søknadHendelseId, søknadDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING")

        assertOppgave(
            Utsett,
            inntektsmeldingDokumentId,
            Inntektsmelding,
            captureslot[0].value()
        )
        assertOppgave(
            Ferdigbehandlet,
            inntektsmeldingDokumentId,
            Inntektsmelding,
            captureslot[1].value()
        )

        assertEquals(2, captureslot.size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_utsatt", inntektsmeldingHendelseId).size)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_kort_periode", inntektsmeldingHendelseId).size)
    }

    @Test
    fun `Forkastet oppgave på inntektsmelding skal opprettes`() {
        val periode1 = UUID.randomUUID()
        val periode2 = UUID.randomUUID()

        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode1)
        sendVedtaksperiodeEndret(
            listOf(inntektsmeldingHendelseId),
            "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING",
            periode1
        )

        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode2)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD", periode2)

        assertEquals(3, captureslot.size)

        assertOppgave(Utsett, inntektsmeldingDokumentId, Inntektsmelding, captureslot[0].value())
        assertOppgave(Ferdigbehandlet, inntektsmeldingDokumentId, Inntektsmelding, captureslot[1].value())
        assertOppgave(Opprett, inntektsmeldingDokumentId, Inntektsmelding, captureslot[2].value())
    }

    @Test
    fun `Sender ikke flere opprett-meldinger hvis vi allerede har forkastet en periode`() {
        val periode1 = UUID.randomUUID()
        val periode2 = UUID.randomUUID()
        val periode3 = UUID.randomUUID()

        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode1)
        sendVedtaksperiodeEndret(
            listOf(inntektsmeldingHendelseId),
            "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING",
            periode1
        )

        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode2)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD", periode2)

        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode3)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD", periode3)

        assertEquals(3, captureslot.size)
    }

    @Test
    fun `Sender kun opprett oppgaver på forkastet inntektsmelding dersom forrige periode var en kort periode`() {
        val periode1 = UUID.randomUUID()
        val periode2 = UUID.randomUUID()
        val periode3 = UUID.randomUUID()

        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode1)
        sendVedtaksperiodeEndret(
            listOf(inntektsmeldingHendelseId),
            "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING",
            periode1
        )

        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode2)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVSLUTTET", periode2)

        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode3)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD", periode3)

        assertEquals(Utsett, captureslot[0].value().oppdateringstype)
        assertEquals(Ferdigbehandlet, captureslot[1].value().oppdateringstype)
        assertEquals(2, captureslot.size)
    }

    @Test
    fun `to korte og en lang periode hvor siste går til infotrygd`() {
        val periode1 = UUID.randomUUID()
        val periode2 = UUID.randomUUID()
        val periode3 = UUID.randomUUID()

        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode1)
        sendVedtaksperiodeEndret(
            listOf(inntektsmeldingHendelseId),
            "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING",
            periode1
        )

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode2)
        sendVedtaksperiodeEndret(
            listOf(inntektsmeldingHendelseId),
            "AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING",
            periode2
        )

        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "AVVENTER_HISTORIKK", periode3)
        sendVedtaksperiodeEndret(listOf(inntektsmeldingHendelseId), "TIL_INFOTRYGD", periode3)

        assertEquals(Utsett, captureslot[0].value().oppdateringstype)
        assertEquals(Ferdigbehandlet, captureslot[1].value().oppdateringstype)
        assertEquals(Opprett, captureslot[2].value().oppdateringstype)
        assertEquals(3, captureslot.size)
    }

    @Test
    fun `utsetter oppgave for inntektsmelding som ikke treffer noen perioder`() {
        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingId = UUID.randomUUID()

        sendInntektsmelding(inntektsmeldingHendelseId, inntektsmeldingId)
        sendInntektsmeldingLagtPåKjøl(inntektsmeldingHendelseId)

        assertEquals(Utsett, captureslot[0].value().oppdateringstype)
        assertEquals(1, rapid.inspektør.events("oppgavestyring_utsatt", inntektsmeldingHendelseId).size)
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
        rapid.sendTestMessage(sendtSøknad(hendelseId, dokumentId))
    }

    fun sendArbeidsgiversøknad(hendelseId: UUID, dokumentId: UUID = UUID.randomUUID()) {
        rapid.sendTestMessage(sendtArbeidsgiversøknad(hendelseId, dokumentId))
    }

    fun sendInntektsmelding(hendelseId: UUID, dokumentId: UUID) {
        rapid.sendTestMessage(inntektsmelding(hendelseId, dokumentId))
    }

    fun sendInntektsmeldingLagtPåKjøl(hendelseId: UUID) {
        rapid.sendTestMessage(inntektsmeldingLagtPåKjøl(hendelseId))
    }

    fun sendVedtaksperiodeEndret(
        hendelseIder: List<UUID>,
        tilstand: String,
        vedtaksperiodeId: UUID = UUID.randomUUID()
    ) {
        rapid.sendTestMessage(vedtaksperiodeEndret(hendelseIder, tilstand, vedtaksperiodeId))
    }
}


fun vedtaksperiodeEndret(
    hendelser: List<UUID>,
    gjeldendeTilstand: String,
    vedtaksperiodeId: UUID
) =
    """{
            "@event_name": "vedtaksperiode_endret",
            "hendelser": ${hendelser.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }},
            "gjeldendeTilstand": "$gjeldendeTilstand",
            "vedtaksperiodeId": "$vedtaksperiodeId"
        }"""

fun inntektsmeldingLagtPåKjøl(
    hendelseId: UUID,
) = """{
            "@event_name": "inntektsmelding_lagt_på_kjøl",
            "hendelseId": "$hendelseId"
        }"""


