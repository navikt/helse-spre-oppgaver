package no.nav.helse

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegistrerInntektsmeldingerTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var hikariConfig: HikariConfig
    private lateinit var dataSource: HikariDataSource
    private lateinit var oppgaveDAO: OppgaveDAO
    private lateinit var registrerInntektsmeldinger: RegistrerInntektsmeldinger
    private val testRapid = TestRapid()

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
        registrerInntektsmeldinger = RegistrerInntektsmeldinger(testRapid, oppgaveDAO)
    }

    @Test
    fun `dytter inntektsmelding inn i db`() {
        val hendelseId = UUID.randomUUID()
        val dokumentId = UUID.randomUUID()
        testRapid.sendTestMessage(inntektsmelding(hendelseId, dokumentId))

        val oppgave = oppgaveDAO.finnOppgave(hendelseId)
        Assertions.assertNotNull(oppgave)
        Assertions.assertEquals(DokumentType.Inntektsmelding, oppgave!!.dokumentType)
    }
}

fun inntektsmelding(
    hendelseId: UUID,
    dokumentId: UUID
) = """{
            "@event_name": "inntektsmelding",
            "@id": "$hendelseId",
            "inntektsmeldingId": "$dokumentId"
        }"""
