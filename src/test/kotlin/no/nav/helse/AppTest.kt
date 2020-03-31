package no.nav.helse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.convertValue
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var hikariConfig: HikariConfig
    private lateinit var dataSource: HikariDataSource
    private lateinit var oppgaveDAO: OppgaveDAO

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
    }

    @Test
    fun `spleis håndterer et helt sykeforløp`() = runBlocking<Unit> {
        val søknad1HendelseId = UUID.randomUUID()
        val søknad1DokumentId = UUID.randomUUID()
        val inntektsmeldingHendelseId = UUID.randomUUID()
        val inntektsmeldingDokumentId = UUID.randomUUID()

        val result = listOf(
            sendtSøknad(søknad1HendelseId, søknad1DokumentId),
            tilstandsendring(listOf(søknad1HendelseId), "AVVENTER_INNTEKTSMELDING_FERDIG_GAP"),
            inntektsmelding(inntektsmeldingHendelseId, inntektsmeldingDokumentId),
            tilstandsendring(listOf(søknad1HendelseId, inntektsmeldingHendelseId), "AVVENTER_SIMULERING"),
            tilstandsendring(
                listOf(søknad1HendelseId, inntektsmeldingHendelseId),
                "AVVENTER_VILKÅRSPRØVING_ARBEIDSGIVERSØKNAD"
            ),
            tilstandsendring(listOf(søknad1HendelseId, inntektsmeldingHendelseId), "AVSLUTTET")
        ).asFlow()
            .oppgaveFlow(oppgaveDAO)
            .toList()

        val expected = listOf(
            Oppgave(
                hendelseId = søknad1HendelseId,
                dokumentId = søknad1DokumentId,
                tilstand = DatabaseTilstand.SpleisLest
            ),
            Oppgave(
                hendelseId = inntektsmeldingHendelseId,
                dokumentId = inntektsmeldingDokumentId,
                tilstand = DatabaseTilstand.SpleisLest
            ),
            Oppgave(
                hendelseId = søknad1HendelseId,
                dokumentId = søknad1DokumentId,
                tilstand = DatabaseTilstand.SpleisFerdigbehandlet
            ),
            Oppgave(
                hendelseId = inntektsmeldingHendelseId,
                dokumentId = inntektsmeldingDokumentId,
                tilstand = DatabaseTilstand.SpleisFerdigbehandlet
            )
        )
        assertEquals(expected, result)
    }

    @Test
    fun `spleis replayer søknad`() = runBlocking<Unit>{
        val søknad1HendelseId = UUID.randomUUID()
        val søknad1DokumentId = UUID.randomUUID()

        val result = listOf(
            sendtSøknad(søknad1HendelseId, søknad1DokumentId),
            tilstandsendring(listOf(søknad1HendelseId), "AVVENTER_INNTEKTSMELDING_FERDIG_GAP"),
            tilstandsendring(listOf(søknad1HendelseId), "AVSLUTTET"),

            sendtSøknad(søknad1HendelseId, søknad1DokumentId),
            tilstandsendring(listOf(søknad1HendelseId), "AVVENTER_INNTEKTSMELDING_FERDIG_GAP"),
            tilstandsendring(listOf(søknad1HendelseId), "AVSLUTTET")
        ).asFlow()
            .oppgaveFlow(oppgaveDAO)
            .toList()

        val expected = listOf(
            Oppgave(
                hendelseId = søknad1HendelseId,
                dokumentId = søknad1DokumentId,
                tilstand = DatabaseTilstand.SpleisLest
            ),
            Oppgave(
                hendelseId = søknad1HendelseId,
                dokumentId = søknad1DokumentId,
                tilstand = DatabaseTilstand.SpleisFerdigbehandlet
            )
        )
        assertEquals(expected, result)
    }

    fun sendtSøknad(
        hendelseId: UUID,
        dokumentId: UUID = UUID.randomUUID()
    ): Pair<String, JsonNode> = "fnr" to objectMapper.convertValue(
        mapOf(
            "@event_type" to "sendt_søknad_nav",
            "@id" to hendelseId,
            "id" to dokumentId
        )
    )

    fun inntektsmelding(
        hendelseId: UUID,
        dokumentId: UUID
    ): Pair<String, JsonNode> = "fnr" to objectMapper.convertValue(
        mapOf(
            "@event_type" to "inntektsmelding",
            "@id" to hendelseId,
            "inntektsmeldingId" to dokumentId
        )
    )

    fun tilstandsendring(
        hendelseIder: List<UUID>,
        gjeldendeTilstand: String
    ): Pair<String, JsonNode> = "fnr" to objectMapper.convertValue(
        mapOf(
            "@event_type" to "vedtaksperiode_endret",
            "hendelser" to hendelseIder,
            "gjeldendeTilstand" to gjeldendeTilstand
        )
    )
}
