package no.nav.helse

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import junit.framework.Assert.assertNull
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class OppgaveDAOTest {
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
    fun `finner ikke en ikke-eksisterende oppgave`() {
        assertNull(oppgaveDAO.finnOppgave(hendelseId = UUID.randomUUID()))
    }

    @Test
    fun `finner en eksisterende oppgave`() {
        val hendelseId = UUID.randomUUID()
        val dokumentId = UUID.randomUUID()
        oppgaveDAO.opprettOppgave(hendelseId = hendelseId, dokumentId = dokumentId)
        assertEquals(
            Oppgave(hendelseId = hendelseId, dokumentId = dokumentId, tilstand = DatabaseTilstand.DokumentOppdaget),
            oppgaveDAO.finnOppgave(hendelseId)
        )
    }
}
