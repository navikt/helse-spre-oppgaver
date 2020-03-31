package no.nav.helse

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.util.UUID
import javax.sql.DataSource

class OppgaveDAO(
    private val dataSource: DataSource
) {
    fun finnOppgave(hendelseId: UUID): Oppgave? = using(sessionOf(dataSource)) { session ->
        session.run(queryOf(
            "SELECT * FROM oppgave_tilstand WHERE hendelse_id=?;",
            hendelseId
        )
            .map { rs ->
                Oppgave(
                    hendelseId = UUID.fromString(rs.string("hendelse_id")),
                    dokumentId = UUID.fromString(rs.string("dokument_id")),
                    tilstand = DatabaseTilstand.valueOf(rs.string("tilstand")),
                    dokumentType = DokumentType.valueOf(rs.string("dokument_type"))
                )
            }
            .asSingle
        )
    }

    fun opprettOppgave(hendelseId: UUID, dokumentId: UUID, dokumentType: DokumentType) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf(
                "INSERT INTO oppgave_tilstand(hendelse_id, dokument_id, dokument_type) VALUES(?, ?, CAST(? AS dokument_type));",
                hendelseId,
                dokumentId,
                dokumentType.name
            ).asUpdate
        )
    }

    fun oppdaterTilstand(hendelseId: UUID, tilTilstand: DatabaseTilstand) = using(sessionOf(dataSource)) { session ->
        session.run(
            queryOf(
                "UPDATE oppgave_tilstand SET tilstand=CAST(? AS tilstand_type) WHERE hendelse_id=?;",
                tilTilstand.name, hendelseId
            ).asUpdate
        )
    }

}
