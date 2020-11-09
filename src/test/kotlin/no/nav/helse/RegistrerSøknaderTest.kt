package no.nav.helse

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.helse.rapids_rivers.inMemoryRapid
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegistrerSøknaderTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var hikariConfig: HikariConfig
    private lateinit var dataSource: HikariDataSource
    private lateinit var oppgaveDAO: OppgaveDAO
    private lateinit var registrerSøknader: RegistrerSøknader
    private val inmemoryrapid = inMemoryRapid {}

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


        registrerSøknader = RegistrerSøknader(inmemoryrapid, oppgaveDAO)
    }

    @Test
    fun `dytter søknader inn i db`() {
        val hendelseId = UUID.randomUUID()
        inmemoryrapid.sendToListeners(sendtSøknad(hendelseId))

        val oppgave = oppgaveDAO.finnOppgave(hendelseId)
        assertNotNull(oppgave)
        assertEquals(DokumentType.Søknad, oppgave!!.dokumentType)
    }
}

fun sendtSøknad(
    hendelseId: UUID,
    dokumentId: UUID = UUID.randomUUID()
): String =
    """{
            "@event_name": "sendt_søknad_nav",
            "@id": "$hendelseId",
            "id": "$dokumentId"
        }"""

fun sendtArbeidsgiversøknad(
    hendelseId: UUID,
    dokumentId: UUID = UUID.randomUUID()
): String =
    """{
            "@event_name": "sendt_søknad_arbeidsgiver",
            "@id": "$hendelseId",
            "id": "$dokumentId"
        }"""

